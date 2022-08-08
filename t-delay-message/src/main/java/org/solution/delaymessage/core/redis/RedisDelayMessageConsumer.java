package org.solution.delaymessage.core.redis;

import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.MarshallingCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.solution.delaymessage.DelayMessageConsumer;
import org.solution.delaymessage.common.DelayMessage;
import org.solution.delaymessage.DelayMessageListener;
import org.springframework.util.Assert;
/**
 * One consumer, one topic.
 */
public class RedisDelayMessageConsumer implements DelayMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisDelayMessageConsumer.class);

    private RedissonClient redissonClient;
    private Codec codec;
    private RBlockingQueue<DelayMessage> blockingQueue;
    private DelayMessageListener delayMessageListener;
    private int consumerSize;

    public RedisDelayMessageConsumer(RedissonClient redissonClient) {
        this(redissonClient, new MarshallingCodec(), 1);
    }

    public RedisDelayMessageConsumer(RedissonClient redissonClient, int consumerSize) {
        this(redissonClient, new MarshallingCodec(), consumerSize);
    }

    public RedisDelayMessageConsumer(RedissonClient redissonClient, Codec codec) {
        this(redissonClient, codec, 1);
    }

    public RedisDelayMessageConsumer(RedissonClient redissonClient, Codec codec, int consumerSize) {
        this.redissonClient = redissonClient;
        this.codec = codec;
        this.consumerSize = consumerSize;
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
        if (this.delayMessageListener != null) {
            throw new IllegalArgumentException("The consumer has registered a message listener!");
        }

        this.delayMessageListener = delayMessageListener;

        for (int i = 0; i < consumerSize; i++) {
            blockingQueue.subscribeOnElements((t) ->{
                LOGGER.debug("Consume message, Thread={}, Message={}", Thread.currentThread().getName(), t);
                delayMessageListener.consumeMessage(t);
            });
        }
    }
}
