package org.solution.delaymessage.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.solution.delaymessage.common.message.DelayMessage;
import org.solution.delaymessage.common.message.DelayMessageExt;
import org.solution.delaymessage.exception.DelayMessagePersistentException;
import org.solution.delaymessage.storage.DelayMessageDbStorageService;
import org.solution.delaymessage.storage.DelayMessageEntity;
import org.solution.delaymessage.storage.DelayMessageRedisStorageService;
import org.solution.delaymessage.utils.MsgIdGenerator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author huxuewang
 */
public class DefaultDelayMessageProduceService implements DelayMessageProduceService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDelayMessageProduceService.class);

    private DelayMessageProducerConfig producerConfig;
    private DelayMessageDbStorageService dbStorageService;
    private DelayMessageRedisStorageService redisStorageService;
    private ThreadPoolTaskExecutor executor;

    public DefaultDelayMessageProduceService(DelayMessageDbStorageService dbStorageService,
                                             DelayMessageRedisStorageService redisStorageService,
                                             DelayMessageProducerConfig producerConfig) {
        this.dbStorageService = dbStorageService;
        this.redisStorageService = redisStorageService;

        if (producerConfig == null) {
            this.producerConfig = new DelayMessageProducerConfig();
        }

        this.executor = buildExecutor(this.producerConfig);
    }

    private ThreadPoolTaskExecutor buildExecutor(DelayMessageProducerConfig producerConfig) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(producerConfig.getCorePoolSize());
        executor.setMaxPoolSize(producerConfig.getCorePoolSize());
        executor.setQueueCapacity(producerConfig.getQueueCapacity());
        executor.setThreadNamePrefix(producerConfig.getThreadNamePrefix());
        executor.setKeepAliveSeconds(producerConfig.getKeepAliveSeconds());
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }

    @Override
    public ProduceResult produceMessage(DelayMessage message, boolean sync) {
        LOGGER.debug("SEND_START: sync - {}, msg - {}", sync, message);
        final DelayMessageExt messageExt = toExt(message);

        if (sync) {
            try {
                saveToDb(messageExt);
            } catch (DelayMessagePersistentException ex) {
                return ProduceResult.fail(ProduceStatus.PERSISTENCE_FAIL);
            }

            CompletableFuture.runAsync(()->{
                redisStorageService.save(messageExt, false);
            }, executor).exceptionally((e)->{
                LOGGER.error("save to redis error, msg-{}, e-{}", messageExt, e);
                return null;
            });
        } else {
            CompletableFuture.runAsync(()->{
                saveToDb(messageExt);
                redisStorageService.save(messageExt, false);
            }, executor).exceptionally((e)->{
                LOGGER.error("save to redis error, msg-{}, e-{}", messageExt, e);
                return null;
            });
        }

        LOGGER.debug("SEND_END: sync - {}, msg - {}", sync, messageExt);
        return ProduceResult.success(messageExt.getId());
    }

    private void saveToDb(DelayMessageExt messageExt) {
        DelayMessageEntity entity = new DelayMessageEntity();
        entity.init(messageExt);

        try {
            dbStorageService.insert(entity);
        } catch (Exception ex) {
            LOGGER.error("Save delay message to db error: {}", ex);
            throw new DelayMessagePersistentException(ex);
        }
    }

    private DelayMessageExt toExt(DelayMessage delayMessage) {
        DelayMessageExt delayMessageExt = new DelayMessageExt();
        delayMessageExt.setTopic(delayMessage.getTopic());
        delayMessageExt.setDelay(delayMessage.getDelay());
        delayMessageExt.setTimeUnit(delayMessage.getTimeUnit());
        delayMessageExt.setProperties(delayMessage.getProperties());
        delayMessageExt.setBody(delayMessage.getBody());
        delayMessageExt.setId(MsgIdGenerator.generate());
        delayMessageExt.setBornTimestamp(System.currentTimeMillis() / 1000);
        return delayMessageExt;
    }

}
