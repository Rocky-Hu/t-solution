package org.solution.delaymessage;

import java.util.concurrent.TimeUnit;

public interface DelayMessageProducer {

    <T> void send(T message, long delay, TimeUnit unit);

}
