package com.hmdp;

import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private RedisIdWorker redisIdWorker;

    @Test
    void test01() {
        for (int i = 0; i < 10; i++) {
            long order = redisIdWorker.nextId("order");
            System.out.println(order);
        }
    }
}
