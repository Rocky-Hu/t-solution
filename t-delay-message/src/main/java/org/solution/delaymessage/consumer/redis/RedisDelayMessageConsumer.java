package org.solution.delaymessage.consumer.redis;

import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.MarshallingCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.solution.delaymessage.DelayMessageConsumer;
import org.solution.delaymessage.DelayMessageListener;
import org.solution.delaymessage.common.message.DelayMessageExt;
import org.solution.delaymessage.consumer.DefaultDelayMessageConsumeService;
import org.solution.delaymessage.consumer.DelayMessageConsumerConfig;
import org.solution.delaymessage.storage.DelayMessageDbStorageService;
import org.springframework.util.Assert;
/**
 * One consumer, one topic.
 */
public class RedisDelayMessageConsumer implements DelayMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisDelayMessageConsumer.class);

    private RedissonClient redissonClient;
    private DefaultDelayMessageConsumeService defaultMessageConsumeService;
    private Codec codec;
    private RBlockingQueue<DelayMessageExt> blockingQueue;

    public RedisDelayMessageConsumer(RedissonClient redissonClient, DelayMessageDbStorageService messageStorageService) {
        this(redissonClient, messageStorageService, new MarshallingCodec(), null);
    }

    public RedisDelayMessageConsumer(RedissonClient redissonClient, DelayMessageDbStorageService messageStorageService,
                                     DelayMessageConsumerConfig consumerConfig) {
        this(redissonClient, messageStorageService, new MarshallingCodec(), consumerConfig);
    }

    public RedisDelayMessageConsumer(RedissonClient redissonClient, DelayMessageDbStorageService messageStorageService, Codec codec) {
        this(redissonClient, messageStorageService, codec, null);
    }

    public RedisDelayMessageConsumer(RedissonClient redissonClient, DelayMessageDbStorageService messageStorageService,
                                     Codec codec, DelayMessageConsumerConfig consumerConfig) {
        this.redissonClient = redissonClient;
        this.codec = codec;
        this.defaultMessageConsumeService = new DefaultDelayMessageConsumeService(messageStorageService, consumerConfig);
    }

    @Override
    public void subscribe(String topic) {
        if (blockingQueue != null) {
            throw new IllegalArgumentException("The consumer has subscribed to a topic!");
        }
        blockingQueue = redissonClient.getBlockingQueue(topic, codec);
    }

    @Override
    public void unsubscribe(String topic) {
        throw new UnsupportedOperationException("Unsupported nonsense operation!");
    }

    @Override
    public void registerMessageListener(DelayMessageListener messageListener) {
        Assert.notNull(blockingQueue, "You must subscribe to a topic first!");
        Assert.notNull(messageListener, "Delay message listener can't be null!");
        this.defaultMessageConsumeService.registerMessageListener(messageListener);

        blockingQueue.subscribeOnElements((t) ->{
            LOGGER.debug("Consume message, {}", t);
            this.defaultMessageConsumeService.consumeMessage(t);
        });
    }
}
