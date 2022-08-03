package org.solution.delaymessage.core.rocketmq;

import org.solution.delaymessage.DelayMessageProducer;

import java.util.concurrent.TimeUnit;

public class RMQDelayMessageProducer implements DelayMessageProducer {

    @Override
    public <T> void send(T message, long delay, TimeUnit unit) {

    }

}
