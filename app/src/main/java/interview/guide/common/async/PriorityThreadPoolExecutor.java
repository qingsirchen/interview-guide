package interview.guide.common.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 优先级线程池执行器
 * 支持任务优先级排序，高优先级任务优先执行
 */
public class PriorityThreadPoolExecutor extends ThreadPoolExecutor {

    private static final Logger log = LoggerFactory.getLogger(PriorityThreadPoolExecutor.class);

    private final String poolName;
    private final AtomicInteger submittedCount = new AtomicInteger(0);
    private final AtomicInteger completedCount = new AtomicInteger(0);
    private final AtomicInteger rejectedCount = new AtomicInteger(0);

    public PriorityThreadPoolExecutor(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            int queueCapacity,
            String poolName) {
        super(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                unit,
                new PriorityBlockingQueue<>(queueCapacity),
                new NamedThreadFactory(poolName),
                new LoggingRejectedHandler(poolName)
        );
        this.poolName = poolName;

        // 允许核心线程超时
        allowCoreThreadTimeOut(true);

        log.info("优先级线程池已创建: name={}, core={}, max={}, queueCapacity={}",
                poolName, corePoolSize, maximumPoolSize, queueCapacity);
    }

    /**
     * 提交优先级任务
     */
    public void execute(Runnable task, TaskPriority priority) {
        execute(task, priority, null);
    }

    /**
     * 提交优先级任务（带任务名称）
     */
    public void execute(Runnable task, TaskPriority priority, String taskName) {
        PriorityTask priorityTask = new PriorityTask(task, priority, taskName);
        submittedCount.incrementAndGet();
        super.execute(priorityTask);
    }

    @Override
    public void execute(Runnable command) {
        // 如果直接提交普通任务，使用默认优先级
        if (command instanceof PriorityTask) {
            submittedCount.incrementAndGet();
            super.execute(command);
        } else {
            execute(command, TaskPriority.NORMAL);
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        completedCount.incrementAndGet();

        if (t != null) {
            if (r instanceof PriorityTask priorityTask) {
                log.error("任务执行异常: {}", priorityTask.getTaskName(), t);
            } else {
                log.error("任务执行异常", t);
            }
        }
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        if (r instanceof PriorityTask priorityTask && priorityTask.getWaitingTime() > 5000) {
            log.warn("任务等待时间过长: {}, waitMs={}",
                    priorityTask.getTaskName(), priorityTask.getWaitingTime());
        }
    }

    /**
     * 获取线程池统计信息
     */
    public PoolStats getStats() {
        return new PoolStats(
                poolName,
                getPoolSize(),
                getActiveCount(),
                getCorePoolSize(),
                getMaximumPoolSize(),
                getQueue().size(),
                submittedCount.get(),
                completedCount.get(),
                rejectedCount.get()
        );
    }

    /**
     * 线程池统计信息
     */
    public record PoolStats(
            String poolName,
            int currentPoolSize,
            int activeCount,
            int corePoolSize,
            int maxPoolSize,
            int queueSize,
            int submitted,
            int completed,
            int rejected
    ) {
        @Override
        public String toString() {
            return String.format(
                    "PoolStats{name='%s', active=%d/%d, queue=%d, submitted=%d, completed=%d, rejected=%d}",
                    poolName, activeCount, currentPoolSize, queueSize, submitted, completed, rejected
            );
        }
    }

    /**
     * 命名线程工厂
     */
    private static class NamedThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        NamedThreadFactory(String poolName) {
            this.namePrefix = poolName + "-thread-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
            t.setDaemon(true);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }

    /**
     * 带日志的拒绝处理器
     * 策略：调用者线程执行 + 记录日志
     */
    private static class LoggingRejectedHandler implements RejectedExecutionHandler {
        private final String poolName;
        private final AtomicInteger rejectedCount = new AtomicInteger(0);

        LoggingRejectedHandler(String poolName) {
            this.poolName = poolName;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            int count = rejectedCount.incrementAndGet();

            String taskInfo = r instanceof PriorityTask pt
                    ? pt.getTaskName() + "(priority=" + pt.getPriority() + ")"
                    : r.toString();

            log.warn("任务被拒绝，将由调用者线程执行: pool={}, task={}, rejectedTotal={}, " +
                            "poolSize={}, activeCount={}, queueSize={}",
                    poolName, taskInfo, count,
                    executor.getPoolSize(), executor.getActiveCount(), executor.getQueue().size());

            // CallerRunsPolicy: 由调用者线程执行
            if (!executor.isShutdown()) {
                r.run();
            }
        }
    }
}
