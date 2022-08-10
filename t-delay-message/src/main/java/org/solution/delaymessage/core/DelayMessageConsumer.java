package org.solution.delaymessage.core;

public interface DelayMessageConsumer {

    void subscribe(String topic);

    void unsubscribe(String topic);

    void registerMessageListener(DelayMessageListener delayMessageListener);

}
