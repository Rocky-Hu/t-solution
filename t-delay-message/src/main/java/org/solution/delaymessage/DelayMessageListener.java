package org.solution.delaymessage;

import org.solution.delaymessage.common.message.DelayMessage;

public interface DelayMessageListener {

    void consumeMessage(DelayMessage delayMessage);

}
