package org.solution.delaymessage.consumer.redis;

import org.redisson.api.RBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.solution.delaymessage.DelayMessageConsumer;
import org.solution.delaymessage.DelayMessageListener;
import org.solution.delaymessage.common.message.DelayMessageExt;
import org.solution.delaymessage.consumer.DefaultDelayMessageConsumeService;
import org.springframework.util.Assert;

/**
 * One consumer, one topic.
 *
 * @author huxuewang
 */
public class RedisDelayMessageConsumer implements DelayMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisDelayMessageConsumer.class);

    private DefaultDelayMessageConsumeService messageConsumeService;
    private RBlockingQueue<DelayMessageExt> blockingQueue;

    public RedisDelayMessageConsumer(DefaultDelayMessageConsumeService messageConsumeService) {
        this.messageConsumeService = messageConsumeService;
    }

    @Override
    public void subscribe(String topic) {
        if (blockingQueue != null) {
            throw new IllegalArgumentException("The consumer has subscribed to a topic!");
        }
        blockingQueue = messageConsumeService.getBlockingQueue(topic);
    }

    @Override
    public void unsubscribe(String topic) {
        throw new UnsupportedOperationException("Unsupported nonsense operation!");
    }

    @Override
    public void registerMessageListener(DelayMessageListener messageListener) {
        Assert.notNull(blockingQueue, "You must subscribe to a topic first!");
        Assert.notNull(messageListener, "Delay message listener can't be null!");
        this.messageConsumeService.registerMessageListener(messageListener);

        blockingQueue.subscribeOnElements((t) ->{
            LOGGER.debug("Consume message, {}", t);
            this.messageConsumeService.consumeMessage(t);
        });
    }
}
