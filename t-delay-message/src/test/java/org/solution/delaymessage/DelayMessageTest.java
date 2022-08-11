package org.solution.delaymessage;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.solution.delaymessage.consumer.DefaultDelayMessageConsumeService;
import org.solution.delaymessage.producer.DefaultDelayMessageProduceService;
import org.solution.delaymessage.producer.DelayMessageProduceService;
import org.solution.delaymessage.storage.DelayMessageDbStorageService;
import org.solution.delaymessage.storage.DelayMessageRedisStorageService;
import org.solution.delaymessage.storage.JdbcDelayMessageDbStorageService;

public abstract class DelayMessageTest {

    public RedissonClient redissonClient;
    public DelayMessageDbStorageService dbStorageService;
    public DelayMessageRedisStorageService redisStorageService;
    public DelayMessageProduceService produceService;
    public DefaultDelayMessageConsumeService consumeService;

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
        consumeService = new DefaultDelayMessageConsumeService(dbStorageService, redisStorageService);
    }

    @AfterEach
    public void setDown() {
        try {
            Thread.sleep(120 * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        redissonClient.shutdown();
    }

}
