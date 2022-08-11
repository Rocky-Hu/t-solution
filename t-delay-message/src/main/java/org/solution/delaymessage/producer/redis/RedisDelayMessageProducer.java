package org.solution.delaymessage.producer.redis;

import org.solution.delaymessage.DelayMessageProducer;
import org.solution.delaymessage.common.message.DelayMessage;
import org.solution.delaymessage.producer.DelayMessageProduceService;
import org.solution.delaymessage.producer.ProduceResult;

/**
 * @author huxuewang
 */
public class RedisDelayMessageProducer implements DelayMessageProducer {

    private DelayMessageProduceService messageProduceService;

    public RedisDelayMessageProducer(DelayMessageProduceService messageProduceService) {
        this.messageProduceService = messageProduceService;
    }

    @Override
    public ProduceResult send(DelayMessage message) {
        return messageProduceService.produceMessage(message, true);
    }

    @Override
    public ProduceResult sendAsync(DelayMessage message) {
        return messageProduceService.produceMessage(message, false);
    }

}
