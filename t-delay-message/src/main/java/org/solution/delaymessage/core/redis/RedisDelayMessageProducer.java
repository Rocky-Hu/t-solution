package org.solution.delaymessage.core.redis;

import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.MarshallingCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.solution.delaymessage.DelayMessageProducer;
import org.solution.delaymessage.common.DelayMessage;

import java.util.concurrent.ConcurrentHashMap;

public class RedisDelayMessageProducer implements DelayMessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisDelayMessageProducer.class);

    private final ConcurrentHashMap<String, Object> parallelLockMap = new ConcurrentHashMap<>();

    private RedissonClient redissonClient;
    private ConcurrentHashMap<String, RDelayedQueue> delayedQueueRegistry = new ConcurrentHashMap<>();
    private Codec codec = new MarshallingCodec();

    public RedisDelayMessageProducer(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    public RedisDelayMessageProducer(RedissonClient redissonClient, Codec codec) {
        this.redissonClient = redissonClient;
        this.codec = codec;
    }

    @Override
    public void send(DelayMessage delayMessage) {
        LOGGER.debug("{}", delayMessage);
        delayedQueue(delayMessage.getTopic()).offer(delayMessage, delayMessage.getDelay(), delayMessage.getTimeUnit());
    }

    @Override
    public void sendAsync(DelayMessage delayMessage) {
        LOGGER.debug("{}", delayMessage);
        delayedQueue(delayMessage.getTopic()).offerAsync(delayMessage, delayMessage.getDelay(), delayMessage.getTimeUnit());
    }

    private RDelayedQueue<DelayMessage> delayedQueue(String name) {
        RDelayedQueue<DelayMessage> delayedQueue = delayedQueueRegistry.get(name);
        if (delayedQueue != null) {
            return delayedQueue;
        } else {
            synchronized (getDelayedQueueInitialLock(name)) {
                delayedQueue = delayedQueueRegistry.get(name);
                if (delayedQueue == null) {
                    RBlockingQueue<DelayMessage> initialBlockingQueue = redissonClient.getBlockingQueue(name);
                    delayedQueue = redissonClient.getDelayedQueue(initialBlockingQueue);
                    delayedQueueRegistry.putIfAbsent(name, delayedQueue);
                }

                return delayedQueue;
            }
        }
    }

    private Object getDelayedQueueInitialLock(String name) {
        Object lock = this;
        if (parallelLockMap != null) {
            Object newLock = new Object();
            lock = parallelLockMap.putIfAbsent(name, newLock);
            if (lock == null) {
                lock = newLock;
            }
        }

        return lock;
    }

}
