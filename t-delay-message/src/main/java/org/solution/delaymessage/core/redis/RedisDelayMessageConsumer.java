package org.solution.delaymessage.core.redis;

import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.MarshallingCodec;
import org.solution.delaymessage.DelayMessageConsumer;
import org.solution.delaymessage.common.DelayMessage;
import org.solution.delaymessage.common.DelayMessageListener;
import org.springframework.util.Assert;

import java.util.concurrent.ConcurrentHashMap;

public class RedisDelayMessageConsumer implements DelayMessageConsumer {

    private RedissonClient redissonClient;
    private Codec codec;
    private ConcurrentHashMap<String, RBlockingQueue<DelayMessage>> blockingQueueRegistry = new ConcurrentHashMap<>();
    private RBlockingQueue<DelayMessage> blockingQueue;
    private int listenerId;

    public RedisDelayMessageConsumer(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
        this.codec = new MarshallingCodec();
    }

    public RedisDelayMessageConsumer(RedissonClient redissonClient, Codec codec) {
        this.redissonClient = redissonClient;
        this.codec = codec;
    }

    @Override
    public void subscribe(String topic) {
        if (blockingQueue != null) {
            throw new IllegalStateException("");
        }
        blockingQueue = redissonClient.getBlockingQueue(topic, codec);
    }

    @Override
    public void unsubscribe(String topic) {

    }

    @Override
    public void registerMessageListener(DelayMessageListener delayMessageListener) {
        Assert.notNull(blockingQueue, "You must subscribe to the topic first!");
        listenerId = blockingQueue.subscribeOnElements(delayMessageListener);
    }

}
