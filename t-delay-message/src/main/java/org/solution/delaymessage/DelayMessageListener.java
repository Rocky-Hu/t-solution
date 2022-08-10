package org.solution.delaymessage;

import org.solution.delaymessage.common.message.DelayMessageExt;

public interface DelayMessageListener {

    void consumeMessage(DelayMessageExt delayMessage);

}
