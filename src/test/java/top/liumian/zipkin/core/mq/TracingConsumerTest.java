package top.liumian.zipkin.core.mq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;
import top.liumian.zipkin.core.mq.consumer.TracingConsumerProxyFactory;

import java.util.List;

/**
 * @author liumian  2022/8/7 12:14
 */
public class TracingConsumerTest extends BaseTracingTest{


    @Test
    public void consumerTest() throws MQClientException, InterruptedException {


        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = TracingConsumerProxyFactory.createTracingConsumer(tracing,new DefaultMQPushConsumer("123"));

        // Specify name server addresses.
        consumer.setNamesrvAddr("localhost:9876");

        // Subscribe one more more topics to consume.
        consumer.subscribe("TopicTest", "*");

        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                System.out.printf("%s - %s Receive New Messages: %s %n", Thread.currentThread().getName(),tracing.tracer().currentSpan().context().traceIdString(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("Consumer Started.%n");

        Thread.sleep(5000L);

    }


}
