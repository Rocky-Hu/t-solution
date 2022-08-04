package org.solution.delaymessage;

import org.solution.delaymessage.common.DelayMessage;

public interface DelayMessageProducer {

    void send(DelayMessage delayMessage);

    void sendAsync(DelayMessage delayMessage);


}
