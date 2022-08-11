package org.solution.delaymessage;

import org.solution.delaymessage.common.message.DelayMessage;
import org.solution.delaymessage.producer.ProduceResult;

/**
 * @author huxuewang
 */
public interface DelayMessageProducer {

    ProduceResult send(DelayMessage message);

    ProduceResult sendAsync(DelayMessage message);

}
