package org.solution.delaymessage;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.solution.delaymessage.consumer.DefaultDelayMessageConsumeService;
import org.solution.delaymessage.consumer.DelayMessageConsumeService;
import org.solution.delaymessage.producer.DefaultDelayMessageProduceService;
import org.solution.delaymessage.producer.DelayMessageProduceService;
import org.solution.delaymessage.producer.ProduceResult;
import org.solution.delaymessage.common.message.DelayMessage;
import org.solution.delaymessage.producer.redis.RedisDelayMessageProducer;
import org.solution.delaymessage.storage.DelayMessageDbStorageService;
import org.solution.delaymessage.storage.DelayMessageRedisStorageService;
import org.solution.delaymessage.storage.JdbcDelayMessageDbStorageService;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisDelayMessageProducerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisDelayMessageConsumerTest.class);

    private RedissonClient redissonClient;
    private DelayMessageDbStorageService dbStorageService;
    private DelayMessageRedisStorageService redisStorageService;
    private DelayMessageProduceService produceService;

    @BeforeEach
    public void setUp() {
        Config config = new Config();
//        config.setNettyThreads(100);
        config.useSingleServer().setAddress("redis://192.168.56.99:6379");
//        config.useSingleServer().setConnectionPoolSize(1000);
        redissonClient = Redisson.create(config);
        redisStorageService = new DelayMessageRedisStorageService(redissonClient);

        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setJdbcUrl("jdbc:postgresql://localhost:5432/soms?currentSchema=public");
        dataSource.setUsername("postgres");
        dataSource.setPassword("14981498");
        dbStorageService = new JdbcDelayMessageDbStorageService(dataSource);

        produceService = new DefaultDelayMessageProduceService(dbStorageService, redisStorageService);
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
    public void test_sendAsync_with_codec() {
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
