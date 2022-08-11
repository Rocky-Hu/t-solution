package org.solution.delaymessage.producer;

import org.solution.delaymessage.common.message.DelayMessage;

/**
 * @author huxuewang
 */
public interface DelayMessageProduceService {

    ProduceResult produceMessage(final DelayMessage delayMessage, boolean sync);

}
