package org.solution.delaymessage.core;

import org.solution.delaymessage.common.message.DelayMessageExt;

public interface DelayMessageListener {

    void consumeMessage(DelayMessageExt delayMessage);

}
