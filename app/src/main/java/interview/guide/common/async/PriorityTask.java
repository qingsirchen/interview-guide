package interview.guide.common.async;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 优先级任务包装类
 * 实现 Comparable 接口，支持优先级队列排序
 */
public class PriorityTask implements Runnable, Comparable<PriorityTask> {

    /**
     * 全局序列号生成器，用于保证同优先级任务的FIFO顺序
     */
    private static final AtomicLong SEQUENCE_GENERATOR = new AtomicLong(0);

    private final Runnable task;
    private final TaskPriority priority;
    private final long sequenceNumber;
    private final long createTime;
    private final String taskName;

    public PriorityTask(Runnable task, TaskPriority priority) {
        this(task, priority, null);
    }

    public PriorityTask(Runnable task, TaskPriority priority, String taskName) {
        this.task = task;
        this.priority = priority;
        this.sequenceNumber = SEQUENCE_GENERATOR.incrementAndGet();
        this.createTime = System.currentTimeMillis();
        this.taskName = taskName != null ? taskName : "task-" + sequenceNumber;
    }

    @Override
    public void run() {
        task.run();
    }

    @Override
    public int compareTo(PriorityTask other) {
        // 1. 先按优先级排序（数值小的优先）
        int priorityCompare = Integer.compare(this.priority.getValue(), other.priority.getValue());
        if (priorityCompare != 0) {
            return priorityCompare;
        }
        // 2. 同优先级按序列号排序（先进先出）
        return Long.compare(this.sequenceNumber, other.sequenceNumber);
    }

    public TaskPriority getPriority() {
        return priority;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public long getCreateTime() {
        return createTime;
    }

    public String getTaskName() {
        return taskName;
    }

    /**
     * 获取任务等待时间（毫秒）
     */
    public long getWaitingTime() {
        return System.currentTimeMillis() - createTime;
    }

    @Override
    public String toString() {
        return String.format("PriorityTask{name='%s', priority=%s, seq=%d, waitMs=%d}",
                taskName, priority, sequenceNumber, getWaitingTime());
    }
}
