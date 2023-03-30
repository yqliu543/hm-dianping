package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @Description:
 * @author: 刘
 * @date: 2023年03月30日 下午 3:01
 */
public class SimpleRedisLock implements ILock{

    private String name;
    private StringRedisTemplate redisTemplate;
    private static final String KEY_PREFIX="lock:";
    private static final String ID_PREFIX= UUID.randomUUID().toString()+"-";
    private static final DefaultRedisScript<Long> unlockScript ;
    static {
        unlockScript=new DefaultRedisScript<>();
        unlockScript.setLocation(new ClassPathResource("unlock.lua"));
        unlockScript.setResultType(Long.class);
    }

    public SimpleRedisLock(String name, StringRedisTemplate redisTemplate) {
        this.name = name;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        String id =ID_PREFIX+ Thread.currentThread().getId();
        Boolean success = redisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, id, timeoutSec, TimeUnit.SECONDS);

        return Boolean.TRUE.equals(success);
    }
    /**
    @Override
    public void unLock() {
        String id =ID_PREFIX+ Thread.currentThread().getId();
        String Threadid = redisTemplate.opsForValue().get(KEY_PREFIX + name);
        if (Threadid.equals(id)) {
            redisTemplate.delete(KEY_PREFIX + name);
        }

    }
    */
    @Override
    public void unLock() {
        redisTemplate.execute(unlockScript, Collections.singletonList(KEY_PREFIX + name),ID_PREFIX+ Thread.currentThread().getId());
    }

}
