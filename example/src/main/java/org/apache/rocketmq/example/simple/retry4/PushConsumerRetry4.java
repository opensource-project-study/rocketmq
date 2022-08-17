package org.apache.rocketmq.example.simple.retry4;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author yuwenbo@kkworld.com
 * @date 2022/8/17
 */
public class PushConsumerRetry4 {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushConsumerRetry4.class);

    private static final int MAX_RECONSUME_TIMES = 20;

    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("simple_message_consumer_group_1");
        consumer.setNamesrvAddr("localhost:9876");
        // 设置Consumer超时时间，单位：分钟
        consumer.setConsumeTimeout(1);
        consumer.subscribe("TopicTest4", "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setMaxReconsumeTimes(MAX_RECONSUME_TIMES);
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                int reconsumeTimes = msgs.get(0).getReconsumeTimes();
                LOGGER.info("retry times: {}", reconsumeTimes);
                if (reconsumeTimes >= MAX_RECONSUME_TIMES) {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }

                // 处理时间超过 设置的超时时间，导致Broker重新push消息，引起消息重试
                // 好像并没有重试，为什么呢？？？
                try {
                    Thread.sleep(TimeUnit.MINUTES.toMillis(5));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
