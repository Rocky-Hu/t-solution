package org.solution.delaymessage.consumer;

import org.redisson.api.RBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.solution.delaymessage.DelayMessageListener;
import org.solution.delaymessage.common.message.DelayMessageExt;
import org.solution.delaymessage.common.message.DelayMessageStatus;
import org.solution.delaymessage.storage.DelayMessageDbStorageService;
import org.solution.delaymessage.storage.DelayMessageRedisStorageService;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author huxuewang
 */
public class DefaultDelayMessageConsumeService implements DelayMessageConsumeService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDelayMessageConsumeService.class);

    private DelayMessageDbStorageService dbStorageService;
    private DelayMessageRedisStorageService redisStorageService;
    private DelayMessageConsumerConfig consumerConfig;
    private DelayMessageListener messageListenerInner;
    private ThreadPoolTaskExecutor consumeExecutor;
    private ThreadPoolTaskExecutor consumeResultExecutor;

    public DefaultDelayMessageConsumeService(DelayMessageDbStorageService dbStorageService, DelayMessageRedisStorageService redisStorageService,
                                             DelayMessageConsumerConfig consumerConfig) {
        this.dbStorageService = dbStorageService;
        this.redisStorageService = redisStorageService;

        if (consumerConfig == null) {
            consumerConfig = new DelayMessageConsumerConfig();
        }

        this.consumerConfig = consumerConfig;
        this.consumeExecutor = buildConsumeExecutor(this.consumerConfig);
        this.consumeResultExecutor = buildConsumeResultExecutor(this.consumerConfig);
    }

    private ThreadPoolTaskExecutor buildConsumeExecutor(DelayMessageConsumerConfig consumerConfig) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(consumerConfig.getCorePoolSize());
        executor.setMaxPoolSize(consumerConfig.getMaxPoolSize());
        executor.setQueueCapacity(consumerConfig.getQueueCapacity());
        executor.setThreadNamePrefix(consumerConfig.getThreadNamePrefix());
        executor.setKeepAliveSeconds(consumerConfig.getKeepAliveSeconds());
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }

    private ThreadPoolTaskExecutor buildConsumeResultExecutor(DelayMessageConsumerConfig consumerConfig) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(consumerConfig.getResultCorePoolSize());
        executor.setMaxPoolSize(consumerConfig.getResultMaxPoolSize());
        executor.setQueueCapacity(consumerConfig.getResultQueueCapacity());
        executor.setThreadNamePrefix(consumerConfig.getResultThreadNamePrefix());
        executor.setKeepAliveSeconds(consumerConfig.getResultKeepAliveSeconds());
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }

    @Override
    public void consumeMessage(final DelayMessageExt msg) {
        CompletableFuture.supplyAsync(()-> this.messageListenerInner.consumeMessage(msg), consumeExecutor).whenCompleteAsync((t, e)->{
            if (t != null) {
                if (t == ConsumeStatus.CONSUME_SUCCESS) {
                    dbStorageService.update(msg.getId(), DelayMessageStatus.CONSUME_SUCCESS.getCode());
                } else if (t == ConsumeStatus.RECONSUME_LATTER) {
                }
            }

            if (e != null) {
                LOGGER.error("Consume message error: msgId-{}, e-{}", msg.getId(), e);
            }
        }, consumeResultExecutor);
    }

    public void registerMessageListener(DelayMessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }

    public RBlockingQueue<DelayMessageExt> getBlockingQueue(String topic) {
        return redisStorageService.getBlockingQueue(topic);
    }

}
