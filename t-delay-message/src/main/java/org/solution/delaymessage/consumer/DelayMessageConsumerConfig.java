package org.solution.delaymessage.consumer;

/**
 * @author huxuewang
 */
public class DelayMessageConsumerConfig {

    /**
     * Consume thread pool config
     */
    private int corePoolSize;
    private int maxPoolSize;
    private int queueCapacity;
    private String threadNamePrefix;
    private int keepAliveSeconds;

    /**
     *  Consume result handle thread pool config
     */
    private int resultCorePoolSize;
    private int resultMaxPoolSize;
    private int resultQueueCapacity;
    private String resultThreadNamePrefix;
    private int resultKeepAliveSeconds;

    public DelayMessageConsumerConfig() {
        this.setCorePoolSize(Runtime.getRuntime().availableProcessors());
        this.setMaxPoolSize(Runtime.getRuntime().availableProcessors() * 2);
        this.setQueueCapacity(10000);
        this.setThreadNamePrefix("DelayMessageConsumerThread-");
        this.setKeepAliveSeconds(60);

        this.setResultCorePoolSize(Runtime.getRuntime().availableProcessors());
        this.setResultMaxPoolSize(Runtime.getRuntime().availableProcessors() * 2);
        this.setResultQueueCapacity(10000);
        this.setResultThreadNamePrefix("DelayMessageConsumerResultThread-");
        this.setResultKeepAliveSeconds(60);
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

    public int getResultCorePoolSize() {
        return resultCorePoolSize;
    }

    public void setResultCorePoolSize(int resultCorePoolSize) {
        this.resultCorePoolSize = resultCorePoolSize;
    }

    public int getResultMaxPoolSize() {
        return resultMaxPoolSize;
    }

    public void setResultMaxPoolSize(int resultMaxPoolSize) {
        this.resultMaxPoolSize = resultMaxPoolSize;
    }

    public int getResultQueueCapacity() {
        return resultQueueCapacity;
    }

    public void setResultQueueCapacity(int resultQueueCapacity) {
        this.resultQueueCapacity = resultQueueCapacity;
    }

    public String getResultThreadNamePrefix() {
        return resultThreadNamePrefix;
    }

    public void setResultThreadNamePrefix(String resultThreadNamePrefix) {
        this.resultThreadNamePrefix = resultThreadNamePrefix;
    }

    public int getResultKeepAliveSeconds() {
        return resultKeepAliveSeconds;
    }

    public void setResultKeepAliveSeconds(int resultKeepAliveSeconds) {
        this.resultKeepAliveSeconds = resultKeepAliveSeconds;
    }

}
