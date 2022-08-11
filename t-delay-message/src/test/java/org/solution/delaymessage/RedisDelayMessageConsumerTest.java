package org.solution.delaymessage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.solution.delaymessage.consumer.ConsumeStatus;
import org.solution.delaymessage.common.message.DelayMessageExt;
import org.solution.delaymessage.consumer.redis.RedisDelayMessageConsumer;

public class RedisDelayMessageConsumerTest {

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
    public void test_orderly_consume() {
        RedisDelayMessageConsumer redisDelayMessageConsumer = new RedisDelayMessageConsumer(redissonClient, 1);
        redisDelayMessageConsumer.subscribe("myTopic");
        redisDelayMessageConsumer.registerMessageListener(new DelayMessageListener() {
            @Override
            public ConsumeStatus consumeMessage(DelayMessageExt message) {
                return ConsumeStatus.CONSUME_SUCCESS;
            }
        });
    }

    @Test
    public void test_concurrently_consume() {
        RedisDelayMessageConsumer redisDelayMessageConsumer = new RedisDelayMessageConsumer(redissonClient, 4);
        redisDelayMessageConsumer.subscribe("myTopic");
        redisDelayMessageConsumer.registerMessageListener(new DelayMessageListener() {
            @Override
            public ConsumeStatus consumeMessage(DelayMessageExt message) {
                return ConsumeStatus.CONSUME_SUCCESS;
            }
        });
    }

}
