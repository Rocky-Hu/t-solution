package org.solution.delaymessage;

import org.junit.jupiter.api.Test;
import org.solution.delaymessage.consumer.ConsumeStatus;
import org.solution.delaymessage.common.message.DelayMessageExt;
import org.solution.delaymessage.consumer.redis.RedisDelayMessageConsumer;

public class DelayMessageConsumerTest extends DelayMessageTest {

    @Test
    public void test_consume() {
        DelayMessageConsumer messageConsumer = new RedisDelayMessageConsumer(consumeService);
        messageConsumer.subscribe("myTopic");
        messageConsumer.registerMessageListener(new DelayMessageListener() {
            @Override
            public ConsumeStatus consumeMessage(DelayMessageExt message) {
                return ConsumeStatus.CONSUME_SUCCESS;
            }
        });
    }

    @Test
    public void test_consume_exception() {
        DelayMessageConsumer messageConsumer = new RedisDelayMessageConsumer(consumeService);
        messageConsumer.subscribe("myTopic");
        messageConsumer.registerMessageListener(new DelayMessageListener() {
            @Override
            public ConsumeStatus consumeMessage(DelayMessageExt message) {
                throw new RuntimeException("xxxx");
            }
        });
    }

}
