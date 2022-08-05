package org.solution.delaymessage;

import org.solution.delaymessage.common.DelayMessage;

public interface DelayMessageListener {

    void consumeMessage(DelayMessage delayMessage);

}
