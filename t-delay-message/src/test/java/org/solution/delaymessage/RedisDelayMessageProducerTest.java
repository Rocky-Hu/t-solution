package org.solution.delaymessage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.solution.delaymessage.common.DelayMessage;
import org.solution.delaymessage.core.redis.RedisDelayMessageProducer;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RedisDelayMessageProducerTest {

    private RedissonClient redissonClient;

    @BeforeEach
    public void setUp() {
        Config config = new Config();
//        config.setNettyThreads(100);
        config.useSingleServer().setAddress("redis://192.168.56.99:6379");
//        config.useSingleServer().setConnectionPoolSize(1000);
        redissonClient = Redisson.create(config);
    }

    @AfterEach
    public void setDown() {
        try {
            Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        redissonClient.shutdown();
    }

    @Test
    public void test_send() {
        RedisDelayMessageProducer redisDelayMessageProducer = new RedisDelayMessageProducer(redissonClient);
        String message = "hello";
        DelayMessage delayMessage = new DelayMessage("myTopic", 20,
                TimeUnit.SECONDS, message.getBytes(StandardCharsets.UTF_8));
        redisDelayMessageProducer.send(delayMessage);
    }

    @Test
    public void test_sendAsync() {
        RedisDelayMessageProducer redisDelayMessageProducer = new RedisDelayMessageProducer(redissonClient);
        String message = "hello";
        DelayMessage delayMessage = new DelayMessage("myTopic", 5,
                TimeUnit.SECONDS, message.getBytes(StandardCharsets.UTF_8));
        redisDelayMessageProducer.sendAsync(delayMessage);
    }

    @Test
    public void test_multiple_thread() throws Exception {
        RedisDelayMessageProducer redisDelayMessageProducer = new RedisDelayMessageProducer(redissonClient);
        CountDownLatch countDownLatch = new CountDownLatch(10);

        for (int i=0;i<1000;i++) {
            final String message = "message-" + i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    countDownLatch.countDown();
                    DelayMessage delayMessage = new DelayMessage("myTopic", 5,
                            TimeUnit.SECONDS, message.getBytes(StandardCharsets.UTF_8));
                    redisDelayMessageProducer.sendAsync(delayMessage);
                }
            }, "thread-" + i).start();
        }

        countDownLatch.await();
    }

}
