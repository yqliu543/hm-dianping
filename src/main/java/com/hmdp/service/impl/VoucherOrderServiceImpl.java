package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private ISeckillVoucherService iSeckillVoucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;
    @Autowired
    private RedissonClient redissonClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private IVoucherOrderService proxy;

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    //    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    //    private class VoucherOrderHandler implements Runnable {
    //        @Override
    //        public void run() {
    //            while (true) {
    //                try {
    //                    //1.获取队列中的订单信息
    //                    VoucherOrder voucherOrder = orderTasks.take();
    //                    //2.创建订单
    //                    handleVoucherOrder(voucherOrder);
    //                } catch (InterruptedException e) {
    //                    e.printStackTrace();
    //                    log.error("处理订单异常", e);
    //                }
    //
    //            }
    //        }
    //    }
    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.order";

        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //1.1判断获取是否成功
                    if (list == null || list.isEmpty()) {
                        //失败继续循环
                        continue;
                    }
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //成功，下单
                    handleVoucherOrder(voucherOrder);
                    //ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("处理订单异常", e);
                    handlePendingList();
                }

            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    //1.获取pending-list队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //1.1判断获取是否成功
                    if (list == null || list.isEmpty()) {
                        //失败继续循环
                        break;
                    }
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //成功，下单
                    handleVoucherOrder(voucherOrder);
                    //ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("处理peding-list订单异常", e);
                }

            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean islock = lock.tryLock();
        if (!islock) {
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder, userId);
        } finally {
            lock.unlock();
        }
    }

    //    @Override
    //    public Result seckillVoucher(Long voucherId) {
    //        SeckillVoucher voucher = iSeckillVoucherService.getById(voucherId);
    //        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
    //            return Result.fail("秒杀尚未开始");
    //        }
    //        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
    //            return Result.fail("秒杀已结束");
    //        }
    //        if (voucher.getStock() < 1) {
    //            return Result.fail("库存不足");
    //        }
    //        Long id = UserHolder.getUser().getId();
    //        //SimpleRedisLock lock = new SimpleRedisLock("order:" + id, stringRedisTemplate);
    //        RLock lock = redissonClient.getLock("lock:order:" + id);
    //        boolean islock = lock.tryLock();
    //        if (!islock){
    //            return Result.fail("不允许重复下单");
    //        }
    //
    //        try {
    //            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
    //            return proxy.createVoucherOrder(voucherId, id);
    //        } finally {
    //            lock.unlock();
    //        }
    //
    //    }
    //    @Override
    //    public Result seckillVoucher(Long voucherId) {
    //        Long userId = UserHolder.getUser().getId();
    //        //1.执行lua脚本
    //        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString());
    //        int r = result.intValue();
    //
    //        //2.判断是否为0
    //        if (r != 0) {
    //            //2.1不为0 代表没有购买资格
    //            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
    //        }
    //        //2.2为0 有购买资格，把下单信息保存到阻塞队列
    //        VoucherOrder voucherOrder = new VoucherOrder();
    //        long order = redisIdWorker.nextId("order");
    //        voucherOrder.setId(order);
    //        voucherOrder.setUserId(userId);
    //        voucherOrder.setVoucherId(voucherId);
    //        orderTasks.add(voucherOrder);
    //        proxy = (IVoucherOrderService) AopContext.currentProxy();
    //        //3.返回订单id
    //        return Result.ok(order);
    //    }
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId));
        int r = result.intValue();

        //2.判断是否为0
        if (r != 0) {
            //2.1不为0 代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        //2.2为0 有购买资格，把下单信息保存到阻塞队列

        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //3.返回订单id
        return Result.ok(orderId);
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder, Long id) {
        Integer count = query().eq("user_id", id).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("该用户已经购买过了");
            return;
        }
        boolean success = iSeckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足");
            return;
        }
        save(voucherOrder);
    }
}
