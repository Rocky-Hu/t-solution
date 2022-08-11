package org.solution.delaymessage;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.solution.delaymessage.producer.ProduceResult;
import org.solution.delaymessage.common.message.DelayMessage;
import org.solution.delaymessage.producer.redis.RedisDelayMessageProducer;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DelayMessageProducerTest extends DelayMessageTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DelayMessageConsumerTest.class);

    @Test
    public void test_send() {
        DelayMessageProducer messageProducer = new RedisDelayMessageProducer(produceService);
        String message = "hello";
        DelayMessage delayMessage = new DelayMessage("myTopic", 5,
                TimeUnit.SECONDS, message.getBytes(StandardCharsets.UTF_8));
        messageProducer.send(delayMessage);
    }

    @Test
    public void test_sendAsync() {
        DelayMessageProducer messageProducer = new RedisDelayMessageProducer(produceService);
        String message = "hello";
        DelayMessage delayMessage = new DelayMessage("myTopic", 5,
                TimeUnit.SECONDS, message.getBytes(StandardCharsets.UTF_8));
        messageProducer.sendAsync(delayMessage);
    }

    @Test
    public void test_multiple_thread() throws Exception {

        AtomicInteger count = new AtomicInteger();

        DelayMessageProducer messageProducer = new RedisDelayMessageProducer(produceService);
        CountDownLatch countDownLatch = new CountDownLatch(10);

        for (int i=0;i<1000;i++) {
            final String message = "message-" + i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    countDownLatch.countDown();
                    DelayMessage delayMessage = new DelayMessage("myTopic", 5,
                            TimeUnit.SECONDS, message.getBytes(StandardCharsets.UTF_8));
                    ProduceResult produceResult = messageProducer.send(delayMessage);
                    LOGGER.info("TEST-RESULT:{}:{}", count.incrementAndGet(), produceResult);
                }
            }, "thread-" + i).start();
        }

        countDownLatch.await();
    }

}
