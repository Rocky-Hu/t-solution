package org.solution.delaymessage;

import org.solution.delaymessage.common.message.DelayMessage;
import org.solution.delaymessage.producer.SendResult;

public interface DelayMessageProducer {

    SendResult send(DelayMessage message);

    SendResult sendAsync(DelayMessage message);

}
