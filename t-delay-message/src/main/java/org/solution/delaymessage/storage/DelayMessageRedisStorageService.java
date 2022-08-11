package org.solution.delaymessage.storage;

import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.solution.delaymessage.common.message.DelayMessageExt;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author huxuewang
 */
public class DelayMessageRedisStorageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DelayMessageRedisStorageService.class);

    private final AtomicInteger regCount = new AtomicInteger();

    private RedissonClient redissonClient;
    private ConcurrentHashMap<String, RDelayedQueue<DelayMessageExt>> delayedQueueRegistry = new ConcurrentHashMap<>();
    private Codec codec;
    private final ConcurrentHashMap<String, Object> parallelLockMap = new ConcurrentHashMap<>();

    public DelayMessageRedisStorageService(RedissonClient redissonClient, Codec codec) {
        this.redissonClient = redissonClient;
        this.codec = codec;
    }

    public void save(DelayMessageExt message, boolean sync) {
        RDelayedQueue<DelayMessageExt> delayedQueue = delayedQueue(message.getTopic());
        if (sync) {
            delayedQueue.offer(message, message.getDelay(), message.getTimeUnit());
        } else {
            delayedQueue.offerAsync(message, message.getDelay(), message.getTimeUnit());
        }
    }

    public RBlockingQueue<DelayMessageExt> getBlockingQueue(String topic) {
        return redissonClient.getBlockingQueue(topic, codec);
    }

    private RDelayedQueue<DelayMessageExt> delayedQueue(String name) {
        RDelayedQueue<DelayMessageExt> delayedQueue = delayedQueueRegistry.get(name);
        if (delayedQueue != null) {
            return delayedQueue;
        } else {
            synchronized (getDelayedQueueInitialLock(name)) {
                delayedQueue = delayedQueueRegistry.get(name);
                if (delayedQueue == null) {
                    RBlockingQueue<DelayMessageExt> initialBlockingQueue = redissonClient.getBlockingQueue(name, codec);
                    delayedQueue = redissonClient.getDelayedQueue(initialBlockingQueue);
                    delayedQueueRegistry.putIfAbsent(name, delayedQueue);
                    LOGGER.info("Register delayed queue, name: {}, count: {}", name, regCount.incrementAndGet());
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
