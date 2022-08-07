package top.liumian.zipkin.core.mq;

import brave.Span;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;
import top.liumian.zipkin.core.mq.producer.TracingProducerProxyFactory;
import top.liumian.zipkin.util.TraceUtil;

import java.io.UnsupportedEncodingException;
import java.util.function.Consumer;

/**
 * @author liumian  2022/8/7 12:14
 */
public class TracingProducerTest extends BaseTracingTest{

    @Test
    public void sendTest() throws MQClientException, UnsupportedEncodingException, MQBrokerException, RemotingException, InterruptedException {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = TracingProducerProxyFactory.createTracingProducer(tracing,new DefaultMQProducer("please_rename_unique_group_name"));
        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        //Launch the instance.
        producer.start();
        TraceUtil.newTrace(tracing, "onsTest", new Consumer<Span>() {
            @Override
            public void accept(Span span) {
                for (int i = 0; i < 100; i++) {
                    //Create a message instance, specifying topic, tag and message body.
                    Message msg = null;
                    try {
                        msg = new Message("TopicTest" /* Topic */,
                                "TagA" /* Tag */,
                                ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                        );
                    } catch (UnsupportedEncodingException e) {
                        throw new RuntimeException(e);
                    }
                    //Call send message to deliver message to one of brokers.
                    SendResult sendResult = null;
                    try {
                        sendResult = producer.send(msg);
                    } catch (MQClientException e) {
                        throw new RuntimeException(e);
                    } catch (RemotingException e) {
                        throw new RuntimeException(e);
                    } catch (MQBrokerException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.printf("%s - %s%n", sendResult,tracing.tracer().currentSpan().context().traceId());
                }
                //Shut down once the producer instance is not longer in use.
            }
        });

        producer.shutdown();
    }

}
