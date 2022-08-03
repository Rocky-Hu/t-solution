package org.solution.delaymessage.core.redis;

import org.redisson.Redisson;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class Consumer {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.56.99:6379");
        RedissonClient redissonClient = Redisson.create(config);
        RBlockingQueue<String> blockingQueue = redissonClient.getBlockingQueue("dest_queue1");
        System.out.println(blockingQueue.take());
    }

}
