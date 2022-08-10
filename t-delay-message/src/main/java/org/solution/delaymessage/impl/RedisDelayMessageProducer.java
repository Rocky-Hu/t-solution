package org.solution.delaymessage.impl;

import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.MarshallingCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.solution.delaymessage.DelayMessageProducer;
import org.solution.delaymessage.common.SendStatus;
import org.solution.delaymessage.common.message.DelayMessage;
import org.solution.delaymessage.common.SendResult;
import org.solution.delaymessage.common.message.DelayMessageExt;
import org.solution.delaymessage.exception.DelayMessagePersistentException;
import org.solution.delaymessage.persistence.DelayMessageEntity;
import org.solution.delaymessage.persistence.DelayMessageService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisDelayMessageProducer implements DelayMessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisDelayMessageProducer.class);

    private final AtomicInteger regCount = new AtomicInteger();

    private final ConcurrentHashMap<String, Object> parallelLockMap = new ConcurrentHashMap<>();

    private RedissonClient redissonClient;
    private DelayMessageService delayMessageService;
    private ConcurrentHashMap<String, RDelayedQueue<DelayMessageExt>> delayedQueueRegistry = new ConcurrentHashMap<>();
    private Codec codec;

    public RedisDelayMessageProducer(RedissonClient redissonClient, DelayMessageService delayMessageService) {
        this(redissonClient, delayMessageService, new MarshallingCodec());
    }

    public RedisDelayMessageProducer(RedissonClient redissonClient, DelayMessageService delayMessageService, Codec codec) {
        this.redissonClient = redissonClient;
        this.delayMessageService = delayMessageService;
        this.codec = codec;
    }

    @Override
    public SendResult send(DelayMessage message) {
        final DelayMessageExt messageExt = toExt(message);

        try {
            saveToDb(messageExt);
        } catch (DelayMessagePersistentException ex) {
            return SendResult.fail(SendStatus.PERSISTENCE_FAIL);
        }

        saveToRedis(messageExt, true);

        LOGGER.debug("send message: {}", messageExt);
        return SendResult.success(messageExt.getId());
    }

    @Override
    public SendResult sendAsync(DelayMessage message) {
        final DelayMessageExt messageExt = toExt(message);

        saveToDb(messageExt);
        saveToRedis(messageExt, false);

        LOGGER.debug("sendAsync message: {}", messageExt);
        return SendResult.success(messageExt.getId());
    }

    private void saveToDb(DelayMessageExt messageExt) {
        DelayMessageEntity delayMessageEntity = new DelayMessageEntity();
        delayMessageEntity.init(messageExt);

        try {
            delayMessageService.insert(delayMessageEntity);
        } catch (Exception ex) {
            LOGGER.error("Save delay message to db error: {}", ex);
            throw new DelayMessagePersistentException(ex);
        }
    }

    private void saveToRedis(DelayMessageExt messageExt, boolean sync) {
        RDelayedQueue<DelayMessageExt> delayedQueue = delayedQueue(messageExt.getTopic());
        if (sync) {
            delayedQueue.offer(messageExt, messageExt.getDelay(), messageExt.getTimeUnit());
        } else {
            delayedQueue.offerAsync(messageExt, messageExt.getDelay(), messageExt.getTimeUnit());
        }
    }

    private DelayMessageExt toExt(DelayMessage delayMessage) {
        DelayMessageExt delayMessageExt = new DelayMessageExt();
        delayMessageExt.setTopic(delayMessage.getTopic());
        delayMessageExt.setDelay(delayMessage.getDelay());
        delayMessageExt.setTimeUnit(delayMessage.getTimeUnit());
        delayMessageExt.setProperties(delayMessage.getProperties());
        delayMessageExt.setBody(delayMessage.getBody());
        delayMessageExt.setId();
        delayMessageExt.setBornTimestamp(System.currentTimeMillis());
        return delayMessageExt;
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
