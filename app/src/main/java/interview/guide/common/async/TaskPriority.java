package interview.guide.common.async;

/**
 * 任务优先级枚举
 * 数值越小优先级越高
 */
public enum TaskPriority {

    /**
     * 最高优先级 - VIP用户、紧急任务
     */
    HIGHEST(0),

    /**
     * 高优先级 - 付费用户
     */
    HIGH(1),

    /**
     * 普通优先级 - 普通用户（默认）
     */
    NORMAL(2),

    /**
     * 低优先级 - 批量任务、后台任务
     */
    LOW(3),

    /**
     * 最低优先级 - 可延迟执行的任务
     */
    LOWEST(4);

    private final int value;

    TaskPriority(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
