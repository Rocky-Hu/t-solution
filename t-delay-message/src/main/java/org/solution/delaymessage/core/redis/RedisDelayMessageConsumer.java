package org.solution.delaymessage.core.redis;

import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.MarshallingCodec;
import org.solution.delaymessage.DelayMessageConsumer;
import org.solution.delaymessage.common.DelayMessage;
import org.solution.delaymessage.DelayMessageListener;
import org.springframework.util.Assert;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One consumer, one topic.
 */
public class RedisDelayMessageConsumer implements DelayMessageConsumer {

    private RedissonClient redissonClient;
    private Codec codec;
    private RBlockingQueue<DelayMessage> blockingQueue;
    private DelayMessageListener delayMessageListener;
    private final ThreadPoolExecutor consumeExecutor;
    private int consumeThreadSize = 1;

    public RedisDelayMessageConsumer(RedissonClient redissonClient) {
        this(redissonClient, new MarshallingCodec());
    }

    public RedisDelayMessageConsumer(RedissonClient redissonClient, Codec codec) {
        this.redissonClient = redissonClient;
        this.codec = codec;
        this.consumeExecutor = initializeConsumeExecutor();
    }

    private ThreadPoolExecutor initializeConsumeExecutor() {
        return new ThreadPoolExecutor(
                consumeThreadSize,
                consumeThreadSize,
                0,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                new ThreadFactoryImpl("DelayMessageConsumeThread_"));
    }

    public int getConsumeThreadSize() {
        return consumeThreadSize;
    }

    public void setConsumeThreadSize(int consumeThreadSize) {
        this.consumeThreadSize = consumeThreadSize;
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
    public void registerMessageListener(DelayMessageListener delayMessageListener) {
        Assert.notNull(blockingQueue, "You must subscribe to a topic first!");
        Assert.notNull(delayMessageListener, "Delay message listener can't be null!");
        if (delayMessageListener != null) {
            throw new IllegalArgumentException("The consumer has registered a message listener!");
        }

        this.delayMessageListener = delayMessageListener;
        for (int i=0; i < consumeThreadSize; i++) {
            consumeExecutor.submit(()->{
                blockingQueue.subscribeOnElements((t) ->{
                    delayMessageListener.consumeMessage(t);
                });
            });
        }
    }

    private class ThreadFactoryImpl implements ThreadFactory {

        private final AtomicLong threadIndex = new AtomicLong(0);
        private final String threadNamePrefix;
        private final boolean daemon;

        public ThreadFactoryImpl(final String threadNamePrefix) {
            this(threadNamePrefix, false);
        }

        public ThreadFactoryImpl(final String threadNamePrefix, boolean daemon) {
            this.threadNamePrefix = threadNamePrefix;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, threadNamePrefix + this.threadIndex.incrementAndGet());
            thread.setDaemon(daemon);
            return thread;
        }

    }

}
