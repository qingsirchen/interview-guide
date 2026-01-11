package interview.guide.common.async;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.concurrent.TimeUnit;

/**
 * 异步线程池配置
 * 提供多个优先级线程池用于不同类型的异步任务
 */
@Configuration
@EnableAsync
@EnableConfigurationProperties(ThreadPoolProperties.class)
public class AsyncConfig {

    private static final Logger log = LoggerFactory.getLogger(AsyncConfig.class);

    private final ThreadPoolProperties properties;

    private PriorityThreadPoolExecutor analyzeExecutor;
    private PriorityThreadPoolExecutor evaluateExecutor;
    private PriorityThreadPoolExecutor vectorizeExecutor;
    private PriorityThreadPoolExecutor aiCallerExecutor;

    public AsyncConfig(ThreadPoolProperties properties) {
        this.properties = properties;
    }

    /**
     * 简历分析线程池
     * 用于消费 Redis Stream 中的简历分析任务
     */
    @Bean(name = "analyzeExecutor")
    public PriorityThreadPoolExecutor analyzeExecutor() {
        var config = properties.analyze();
        this.analyzeExecutor = new PriorityThreadPoolExecutor(
                config.coreSize(),
                config.maxSize(),
                config.keepAliveSeconds(),
                TimeUnit.SECONDS,
                config.queueCapacity(),
                "analyze-pool"
        );
        return analyzeExecutor;
    }

    /**
     * 面试评估线程池
     * 用于消费 Redis Stream 中的面试评估任务
     */
    @Bean(name = "evaluateExecutor")
    public PriorityThreadPoolExecutor evaluateExecutor() {
        var config = properties.evaluate();
        this.evaluateExecutor = new PriorityThreadPoolExecutor(
                config.coreSize(),
                config.maxSize(),
                config.keepAliveSeconds(),
                TimeUnit.SECONDS,
                config.queueCapacity(),
                "evaluate-pool"
        );
        return evaluateExecutor;
    }

    /**
     * 向量化线程池
     * 用于消费 Redis Stream 中的知识库向量化任务
     */
    @Bean(name = "vectorizeExecutor")
    public PriorityThreadPoolExecutor vectorizeExecutor() {
        var config = properties.vectorize();
        this.vectorizeExecutor = new PriorityThreadPoolExecutor(
                config.coreSize(),
                config.maxSize(),
                config.keepAliveSeconds(),
                TimeUnit.SECONDS,
                config.queueCapacity(),
                "vectorize-pool"
        );
        return vectorizeExecutor;
    }

    /**
     * AI调用线程池
     * 专用于调用外部 AI API（如 Gemini），支持并发 IO 等待
     */
    @Bean(name = "aiCallerExecutor")
    public PriorityThreadPoolExecutor aiCallerExecutor() {
        var config = properties.aiCaller();
        this.aiCallerExecutor = new PriorityThreadPoolExecutor(
                config.coreSize(),
                config.maxSize(),
                config.keepAliveSeconds(),
                TimeUnit.SECONDS,
                config.queueCapacity(),
                "ai-caller-pool"
        );
        return aiCallerExecutor;
    }

    @PreDestroy
    public void shutdown() {
        log.info("开始关闭线程池...");

        shutdownPool(analyzeExecutor, "analyze-pool");
        shutdownPool(evaluateExecutor, "evaluate-pool");
        shutdownPool(vectorizeExecutor, "vectorize-pool");
        shutdownPool(aiCallerExecutor, "ai-caller-pool");

        log.info("所有线程池已关闭");
    }

    private void shutdownPool(PriorityThreadPoolExecutor executor, String name) {
        if (executor == null) return;

        try {
            log.info("关闭线程池: {}, stats={}", name, executor.getStats());
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("线程池 {} 未能在30秒内关闭，强制关闭", name);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("关闭线程池 {} 时被中断", name, e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
