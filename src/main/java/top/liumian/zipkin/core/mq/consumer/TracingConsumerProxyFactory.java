package top.liumian.zipkin.core.mq.consumer;

import brave.Tracing;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import top.liumian.zipkin.core.mq.TracingProxyFactory;

import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author liumian  2022/8/7 11:17
 */
public class TracingConsumerProxyFactory extends TracingProxyFactory<DefaultMQPushConsumer> {

    private final static Logger logger = Logger.getLogger(TracingConsumerProxyFactory.class.getName());

    public static DefaultMQPushConsumer createTracingConsumer(Tracing tracing, DefaultMQPushConsumer consumer) {
        return new TracingConsumerProxyFactory(tracing, consumer).getProxy();
    }

    private TracingConsumerProxyFactory(Tracing tracing, DefaultMQPushConsumer consumer) {
        super(tracing, consumer);
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        if (isTargetMethod(method)) {
            if (args[0] instanceof MessageListenerConcurrently) {
                return method.invoke(instance, new TracingMessageListenerConcurrently(tracing, (MessageListenerConcurrently) args[0]));
            } else {
                return method.invoke(instance, new TracingMessageListenerOrderly(tracing, (TracingMessageListenerOrderly) args[0]));
            }
        } else {
            return method.invoke(instance, args);
        }
    }

    /**
     * 判断是否是目标代理方法
     *
     * @param method 当前调用方法
     * @return 判断结果
     */
    private boolean isTargetMethod(Method method) {
        String methodName = method.getName();
        if ("registerMessageListener".equals(methodName)) {
            if (instance.getConsumeMessageBatchMaxSize() > 1) {
                logger.log(Level.ALL, "最大批量消费消息数量大于1，链路跟踪不生效");
                return false;
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

}
