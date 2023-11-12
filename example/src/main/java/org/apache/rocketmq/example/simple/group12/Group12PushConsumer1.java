package org.apache.rocketmq.example.simple.group12;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.example.common.ExampleConst;

/**
 * @author yyuweb@outlook.com
 * @date 2023/11/12
 */
public class Group12PushConsumer1 {

    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer1 = new DefaultMQPushConsumer(ExampleConst.SIMPLE_CONSUMER_GROUP_012);
        consumer1.setNamesrvAddr("localhost:9876");
        consumer1.subscribe("TopicTest12", "*");
        consumer1.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer1.setConsumeThreadMin(1);
        consumer1.setConsumeThreadMax(2);
        consumer1.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s consumer1 Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer(ExampleConst.SIMPLE_CONSUMER_GROUP_012);
        consumer2.setNamesrvAddr("localhost:9876");
        consumer2.subscribe("TopicTest12", "*");
        consumer2.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer2.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s consumer2 Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer1.start();
        consumer2.start();
        System.out.printf("Consumer Started.%n");
    }
}
