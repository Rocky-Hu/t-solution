package org.solution.delaymessage.producer;

/**
 * @author huxuewang
 */
public class DelayMessageProducerConfig {
    private int corePoolSize;
    private int maxPoolSize;
    private int queueCapacity;
    private String threadNamePrefix;
    private int keepAliveSeconds;

    public DelayMessageProducerConfig() {
        this.corePoolSize = Runtime.getRuntime().availableProcessors();
        this.maxPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        this.queueCapacity = 10000;
        this.threadNamePrefix = "DelayMessageProducerThread-";
        this.keepAliveSeconds = 60;
    }

    public DelayMessageProducerConfig(int corePoolSize, int maxPoolSize, int queueCapacity, String threadNamePrefix, int keepAliveSeconds) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.queueCapacity = queueCapacity;
        this.threadNamePrefix = threadNamePrefix;
        this.keepAliveSeconds = keepAliveSeconds;
    }

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public void setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public String getThreadNamePrefix() {
        return threadNamePrefix;
    }

    public void setThreadNamePrefix(String threadNamePrefix) {
        this.threadNamePrefix = threadNamePrefix;
    }

    public int getKeepAliveSeconds() {
        return keepAliveSeconds;
    }

    public void setKeepAliveSeconds(int keepAliveSeconds) {
        this.keepAliveSeconds = keepAliveSeconds;
    }

}
