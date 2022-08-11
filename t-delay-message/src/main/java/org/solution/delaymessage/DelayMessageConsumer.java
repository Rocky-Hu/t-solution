package org.solution.delaymessage;

/**
 * @author huxuewang
 */
public interface DelayMessageConsumer {

    void subscribe(String topic);

    void unsubscribe(String topic);

    void registerMessageListener(DelayMessageListener messageListener);

}
