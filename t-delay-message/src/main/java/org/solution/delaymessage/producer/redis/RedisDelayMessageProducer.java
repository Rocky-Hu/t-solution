package org.solution.delaymessage.producer.redis;

import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.MarshallingCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.solution.delaymessage.DelayMessageProducer;
import org.solution.delaymessage.common.message.DelayMessage;
import org.solution.delaymessage.common.message.DelayMessageConstant;
import org.solution.delaymessage.common.message.DelayMessageExt;
import org.solution.delaymessage.exception.DelayMessagePersistentException;
import org.solution.delaymessage.producer.DelayMessageProducerConfig;
import org.solution.delaymessage.producer.SendResult;
import org.solution.delaymessage.producer.SendStatus;
import org.solution.delaymessage.storage.DelayMessageEntity;
import org.solution.delaymessage.storage.DelayMessageDbStorageService;
import org.solution.delaymessage.utils.MsgIdGenerator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisDelayMessageProducer implements DelayMessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisDelayMessageProducer.class);

    private final AtomicInteger regCount = new AtomicInteger();

    private final ConcurrentHashMap<String, Object> parallelLockMap = new ConcurrentHashMap<>();

    private RedissonClient redissonClient;
    private DelayMessageDbStorageService messageStorageService;
    private ConcurrentHashMap<String, RDelayedQueue<DelayMessageExt>> delayedQueueRegistry = new ConcurrentHashMap<>();
    private Codec codec;
    private ThreadPoolTaskExecutor executor;

    public RedisDelayMessageProducer(RedissonClient redissonClient, DelayMessageDbStorageService messageStorageService) {
        this(redissonClient, messageStorageService, new MarshallingCodec(), null);
    }

    public RedisDelayMessageProducer(RedissonClient redissonClient, DelayMessageDbStorageService messageStorageService, Codec codec) {
        this(redissonClient, messageStorageService, codec, null);
    }

    public RedisDelayMessageProducer(RedissonClient redissonClient, DelayMessageDbStorageService messageStorageService, DelayMessageProducerConfig producerConfig) {
        this(redissonClient, messageStorageService, new MarshallingCodec(), producerConfig);
    }

    public RedisDelayMessageProducer(RedissonClient redissonClient, DelayMessageDbStorageService messageStorageService, Codec codec, DelayMessageProducerConfig producerConfig) {
        this.redissonClient = redissonClient;
        this.messageStorageService = messageStorageService;
        this.codec = codec;
        this.executor = buildExecutor(producerConfig);
    }

    private ThreadPoolTaskExecutor buildExecutor(DelayMessageProducerConfig producerConfig) {

        if (producerConfig == null) {
            producerConfig = new DelayMessageProducerConfig(
                    Runtime.getRuntime().availableProcessors(),
                    Runtime.getRuntime().availableProcessors() * 2,
                    10000,
                    "DelayMessageProducerThread-",
                    60);
        }

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(producerConfig.getCorePoolSize());
        executor.setMaxPoolSize(producerConfig.getCorePoolSize());
        executor.setQueueCapacity(producerConfig.getQueueCapacity());
        executor.setThreadNamePrefix(producerConfig.getThreadNamePrefix());
        executor.setKeepAliveSeconds(producerConfig.getKeepAliveSeconds());
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }

    @Override
    public SendResult send(DelayMessage message) {
        return innerSend(message, true);
    }

    @Override
    public SendResult sendAsync(DelayMessage message) {
        return innerSend(message, false);
    }

    private SendResult innerSend(DelayMessage message, boolean sync) {
        MDC.put(DelayMessageConstant.TRACE_ID, UUID.randomUUID().toString());
        LOGGER.debug("SEND_START: sync - {}, msg - {}", sync, message);
        final DelayMessageExt messageExt = toExt(message);

        if (sync) {
            try {
                saveToDb(messageExt);
            } catch (DelayMessagePersistentException ex) {
                return SendResult.fail(SendStatus.PERSISTENCE_FAIL);
            }

            CompletableFuture.runAsync(()->{
                saveToRedis(messageExt, false);
            }, executor).exceptionally((e)->{
                LOGGER.error("save to redis error, msg-{}, e-{}", messageExt, e);
                return null;
            });
        } else {
            CompletableFuture.runAsync(()->{
                saveToDb(messageExt);
                saveToRedis(messageExt, false);
            }, executor).exceptionally((e)->{
                LOGGER.error("save to redis error, msg-{}, e-{}", messageExt, e);
                return null;
            });
        }

        LOGGER.debug("SEND_END: sync - {}, msg - {}", sync, messageExt);
        MDC.remove(DelayMessageConstant.TRACE_ID);
        return SendResult.success(messageExt.getId());
    }

    private void saveToDb(DelayMessageExt messageExt) {
        DelayMessageEntity entity = new DelayMessageEntity();
        entity.init(messageExt);

        try {
            messageStorageService.insert(entity);
        } catch (Exception ex) {
            LOGGER.error("Save delay message to db error: {}", ex);
            throw new DelayMessagePersistentException(ex);
        }
    }

    private void saveToRedis(DelayMessageExt message, boolean sync) {
        RDelayedQueue<DelayMessageExt> delayedQueue = delayedQueue(message.getTopic());
        if (sync) {
            delayedQueue.offer(message, message.getDelay(), message.getTimeUnit());
        } else {
            delayedQueue.offerAsync(message, message.getDelay(), message.getTimeUnit());
        }
    }

    private DelayMessageExt toExt(DelayMessage delayMessage) {
        DelayMessageExt delayMessageExt = new DelayMessageExt();
        delayMessageExt.setTopic(delayMessage.getTopic());
        delayMessageExt.setDelay(delayMessage.getDelay());
        delayMessageExt.setTimeUnit(delayMessage.getTimeUnit());
        delayMessageExt.setProperties(delayMessage.getProperties());
        delayMessageExt.setBody(delayMessage.getBody());
        delayMessageExt.setId(MsgIdGenerator.generate());
        delayMessageExt.setBornTimestamp(System.currentTimeMillis() / 1000);
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
