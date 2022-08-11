package org.solution.delaymessage;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.solution.delaymessage.consumer.ConsumeStatus;
import org.solution.delaymessage.common.message.DelayMessageExt;
import org.solution.delaymessage.consumer.DefaultDelayMessageConsumeService;
import org.solution.delaymessage.consumer.redis.RedisDelayMessageConsumer;
import org.solution.delaymessage.storage.DelayMessageDbStorageService;
import org.solution.delaymessage.storage.DelayMessageRedisStorageService;
import org.solution.delaymessage.storage.JdbcDelayMessageDbStorageService;

public class RedisDelayMessageConsumerTest {

    private RedissonClient redissonClient;
    private DelayMessageDbStorageService dbStorageService;
    private DelayMessageRedisStorageService redisStorageService;
    private DefaultDelayMessageConsumeService consumeService;

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

        consumeService = new DefaultDelayMessageConsumeService(dbStorageService, redisStorageService);
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
        DelayMessageConsumer messageConsumer = new RedisDelayMessageConsumer(consumeService);
        messageConsumer.subscribe("myTopic");
        messageConsumer.registerMessageListener(new DelayMessageListener() {
            @Override
            public ConsumeStatus consumeMessage(DelayMessageExt message) {
                return ConsumeStatus.CONSUME_SUCCESS;
            }
        });
    }

    @Test
    public void test_concurrently_consume() {
        DelayMessageConsumer messageConsumer = new RedisDelayMessageConsumer(consumeService);
        messageConsumer.subscribe("myTopic");
        messageConsumer.registerMessageListener(new DelayMessageListener() {
            @Override
            public ConsumeStatus consumeMessage(DelayMessageExt message) {
                return ConsumeStatus.CONSUME_SUCCESS;
            }
        });
    }

}
