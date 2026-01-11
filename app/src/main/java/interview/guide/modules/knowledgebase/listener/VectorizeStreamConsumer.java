package interview.guide.modules.knowledgebase.listener;

import interview.guide.common.async.PriorityThreadPoolExecutor;
import interview.guide.common.async.TaskPriority;
import interview.guide.common.constant.AsyncTaskStreamConstants;
import interview.guide.infrastructure.redis.RedisService;
import interview.guide.modules.knowledgebase.model.VectorStatus;
import interview.guide.modules.knowledgebase.repository.KnowledgeBaseRepository;
import interview.guide.modules.knowledgebase.service.KnowledgeBaseVectorService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.stream.StreamMessageId;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 知识库向量化 Stream 消费者
 * 负责从 Redis Stream 消费消息并执行向量化
 *
 * 优化点：
 * 1. 使用多线程消费者并发读取消息
 * 2. 使用优先级线程池执行向量化任务
 * 3. 支持任务优先级
 */
@Slf4j
@Component
public class VectorizeStreamConsumer {

    private final RedisService redisService;
    private final KnowledgeBaseVectorService vectorService;
    private final KnowledgeBaseRepository knowledgeBaseRepository;
    private final PriorityThreadPoolExecutor vectorizeExecutor;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService consumerExecutor;
    private String consumerName;

    /**
     * 消费者线程数量
     */
    private static final int CONSUMER_THREAD_COUNT = 2;

    public VectorizeStreamConsumer(
            RedisService redisService,
            KnowledgeBaseVectorService vectorService,
            KnowledgeBaseRepository knowledgeBaseRepository,
            @Qualifier("vectorizeExecutor") PriorityThreadPoolExecutor vectorizeExecutor) {
        this.redisService = redisService;
        this.vectorService = vectorService;
        this.knowledgeBaseRepository = knowledgeBaseRepository;
        this.vectorizeExecutor = vectorizeExecutor;
    }

    @PostConstruct
    public void init() {
        // 生成唯一的消费者名称（支持多实例部署）
        this.consumerName = AsyncTaskStreamConstants.KB_VECTORIZE_CONSUMER_PREFIX + UUID.randomUUID().toString().substring(0, 8);

        // 创建消费者组（如果不存在）
        try {
            redisService.createStreamGroup(
                AsyncTaskStreamConstants.KB_VECTORIZE_STREAM_KEY,
                AsyncTaskStreamConstants.KB_VECTORIZE_GROUP_NAME
            );
            log.info("Redis Stream 消费者组已创建或已存在: {}", AsyncTaskStreamConstants.KB_VECTORIZE_GROUP_NAME);
        } catch (Exception e) {
            log.warn("创建消费者组时发生异常（可能已存在）: {}", e.getMessage());
        }

        // 启动多个消费者线程
        this.consumerExecutor = Executors.newFixedThreadPool(CONSUMER_THREAD_COUNT, r -> {
            Thread t = new Thread(r, "vectorize-consumer-" + UUID.randomUUID().toString().substring(0, 4));
            t.setDaemon(true);
            return t;
        });

        running.set(true);

        // 启动消费者线程
        for (int i = 0; i < CONSUMER_THREAD_COUNT; i++) {
            consumerExecutor.submit(this::consumeLoop);
        }

        log.info("向量化消费者已启动: consumerName={}, consumerThreads={}, vectorizePool={}",
                consumerName, CONSUMER_THREAD_COUNT, vectorizeExecutor.getStats());
    }

    @PreDestroy
    public void shutdown() {
        running.set(false);

        if (consumerExecutor != null) {
            consumerExecutor.shutdown();
            try {
                if (!consumerExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    consumerExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                consumerExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        log.info("向量化消费者已关闭: consumerName={}", consumerName);
    }

    /**
     * 消费循环
     */
    private void consumeLoop() {
        String threadName = Thread.currentThread().getName();
        log.info("消费者线程已启动: {}", threadName);

        while (running.get()) {
            try {
                redisService.streamConsumeMessages(
                    AsyncTaskStreamConstants.KB_VECTORIZE_STREAM_KEY,
                    AsyncTaskStreamConstants.KB_VECTORIZE_GROUP_NAME,
                    consumerName + "-" + threadName,
                    AsyncTaskStreamConstants.BATCH_SIZE,
                    AsyncTaskStreamConstants.POLL_INTERVAL_MS,
                    this::dispatchToExecutor
                );
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    log.info("消费者线程被中断: {}", threadName);
                    break;
                }
                log.error("消费消息时发生错误: thread={}, error={}", threadName, e.getMessage(), e);
            }
        }

        log.info("消费者线程已退出: {}", threadName);
    }

    /**
     * 将消息分发到优先级线程池执行
     */
    private void dispatchToExecutor(StreamMessageId messageId, Map<String, String> data) {
        String kbIdStr = data.get(AsyncTaskStreamConstants.FIELD_KB_ID);
        String priorityStr = data.getOrDefault(AsyncTaskStreamConstants.FIELD_PRIORITY, TaskPriority.NORMAL.name());

        if (kbIdStr == null) {
            log.warn("消息格式错误，跳过: messageId={}", messageId);
            ackMessage(messageId);
            return;
        }

        // 解析优先级
        TaskPriority priority;
        try {
            priority = TaskPriority.valueOf(priorityStr);
        } catch (IllegalArgumentException e) {
            priority = TaskPriority.NORMAL;
        }

        String taskName = "vectorize-kb-" + kbIdStr;

        // 提交到优先级线程池
        vectorizeExecutor.execute(() -> processMessage(messageId, data), priority, taskName);

        log.debug("任务已提交到线程池: kbId={}, priority={}", kbIdStr, priority);
    }

    /**
     * 处理单条消息
     */
    private void processMessage(StreamMessageId messageId, Map<String, String> data) {
        String kbIdStr = data.get(AsyncTaskStreamConstants.FIELD_KB_ID);
        String content = data.get(AsyncTaskStreamConstants.FIELD_CONTENT);
        String retryCountStr = data.getOrDefault(AsyncTaskStreamConstants.FIELD_RETRY_COUNT, "0");

        if (kbIdStr == null || content == null) {
            log.warn("消息格式错误，跳过: messageId={}", messageId);
            ackMessage(messageId);
            return;
        }

        Long kbId = Long.parseLong(kbIdStr);
        int retryCount = Integer.parseInt(retryCountStr);

        log.info("开始处理向量化任务: kbId={}, messageId={}, retryCount={}, thread={}",
                kbId, messageId, retryCount, Thread.currentThread().getName());

        try {
            // 1. 更新状态为 PROCESSING
            updateVectorStatus(kbId, VectorStatus.PROCESSING, null);

            // 2. 执行向量化
            vectorService.vectorizeAndStore(kbId, content);

            // 3. 更新状态为 COMPLETED
            updateVectorStatus(kbId, VectorStatus.COMPLETED, null);

            // 4. 确认消息
            ackMessage(messageId);

            log.info("向量化任务完成: kbId={}", kbId);

        } catch (Exception e) {
            log.error("向量化任务失败: kbId={}, error={}", kbId, e.getMessage(), e);

            // 判断是否需要重试
            if (retryCount < AsyncTaskStreamConstants.MAX_RETRY_COUNT) {
                // 重新入队（增加重试计数）
                retryMessage(kbId, content, retryCount + 1, data.get(AsyncTaskStreamConstants.FIELD_PRIORITY));
            } else {
                // 超过最大重试次数，标记为失败
                String errorMsg = truncateError("向量化失败(已重试" + retryCount + "次): " + e.getMessage());
                updateVectorStatus(kbId, VectorStatus.FAILED, errorMsg);
            }

            // 确认原消息（无论成功失败都要确认，否则会重复消费）
            ackMessage(messageId);
        }
    }

    /**
     * 重试消息（重新发送到 Stream）
     */
    private void retryMessage(Long kbId, String content, int retryCount, String priority) {
        try {
            Map<String, String> message = Map.of(
                AsyncTaskStreamConstants.FIELD_KB_ID, kbId.toString(),
                AsyncTaskStreamConstants.FIELD_CONTENT, content,
                AsyncTaskStreamConstants.FIELD_RETRY_COUNT, String.valueOf(retryCount),
                AsyncTaskStreamConstants.FIELD_PRIORITY, priority != null ? priority : TaskPriority.NORMAL.name()
            );

            redisService.streamAdd(AsyncTaskStreamConstants.KB_VECTORIZE_STREAM_KEY, message);
            log.info("向量化任务已重新入队: kbId={}, retryCount={}", kbId, retryCount);

        } catch (Exception e) {
            log.error("重试入队失败: kbId={}, error={}", kbId, e.getMessage(), e);
            updateVectorStatus(kbId, VectorStatus.FAILED, truncateError("重试入队失败: " + e.getMessage()));
        }
    }

    /**
     * 确认消息
     */
    private void ackMessage(StreamMessageId messageId) {
        try {
            redisService.streamAck(
                AsyncTaskStreamConstants.KB_VECTORIZE_STREAM_KEY,
                AsyncTaskStreamConstants.KB_VECTORIZE_GROUP_NAME,
                messageId
            );
        } catch (Exception e) {
            log.error("确认消息失败: messageId={}, error={}", messageId, e.getMessage(), e);
        }
    }

    /**
     * 更新向量化状态
     */
    private void updateVectorStatus(Long kbId, VectorStatus status, String error) {
        try {
            knowledgeBaseRepository.findById(kbId).ifPresent(kb -> {
                kb.setVectorStatus(status);
                kb.setVectorError(error);
                knowledgeBaseRepository.save(kb);
                log.debug("向量化状态已更新: kbId={}, status={}", kbId, status);
            });
        } catch (Exception e) {
            log.error("更新向量化状态失败: kbId={}, status={}, error={}", kbId, status, e.getMessage(), e);
        }
    }

    /**
     * 截断错误信息，避免超过数据库字段长度
     */
    private String truncateError(String error) {
        if (error == null) return null;
        return error.length() > 500 ? error.substring(0, 500) : error;
    }
}
