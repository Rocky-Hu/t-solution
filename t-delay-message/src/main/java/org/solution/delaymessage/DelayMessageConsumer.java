package org.solution.delaymessage;

import org.solution.delaymessage.common.DelayMessageListener;

public interface DelayMessageConsumer {

    void subscribe(String topic);

    void unsubscribe(String topic);

    void registerMessageListener(DelayMessageListener delayMessageListener);

}
