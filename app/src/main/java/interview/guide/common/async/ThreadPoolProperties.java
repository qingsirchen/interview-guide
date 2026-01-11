package interview.guide.common.async;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 线程池配置属性
 */
@ConfigurationProperties(prefix = "app.thread-pool")
public record ThreadPoolProperties(
        PoolConfig analyze,
        PoolConfig evaluate,
        PoolConfig vectorize,
        PoolConfig aiCaller
) {
    public ThreadPoolProperties {
        // 默认值
        if (analyze == null) {
            analyze = new PoolConfig(2, 4, 100, 60);
        }
        if (evaluate == null) {
            evaluate = new PoolConfig(2, 4, 100, 60);
        }
        if (vectorize == null) {
            vectorize = new PoolConfig(2, 4, 100, 60);
        }
        if (aiCaller == null) {
            aiCaller = new PoolConfig(4, 8, 200, 120);
        }
    }

    /**
     * 单个线程池配置
     */
    public record PoolConfig(
            int coreSize,
            int maxSize,
            int queueCapacity,
            int keepAliveSeconds
    ) {
        public PoolConfig {
            // 默认值
            if (coreSize <= 0) coreSize = 2;
            if (maxSize <= 0) maxSize = 4;
            if (queueCapacity <= 0) queueCapacity = 100;
            if (keepAliveSeconds <= 0) keepAliveSeconds = 60;
        }
    }
}
