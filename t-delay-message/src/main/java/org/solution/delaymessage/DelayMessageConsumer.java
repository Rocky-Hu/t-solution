package org.solution.delaymessage;

public interface DelayMessageConsumer {

    void subscribe(String topic);

    void unsubscribe(String topic);

    void registerMessageListener(DelayMessageListener delayMessageListener);

}
