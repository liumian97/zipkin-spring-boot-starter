package top.liumian.zipkin.core.mq.consumer;

import brave.Tracing;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;

import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author liumian  2022/8/7 11:17
 */
public class TracingConsumerProxyFactory implements MethodInterceptor {

    private final static Logger logger = Logger.getLogger(TracingConsumerProxyFactory.class.getName());

    private final Tracing tracing;

    private final DefaultMQPushConsumer consumer;

    public static DefaultMQPushConsumer createTracingConsumer(Tracing tracing, DefaultMQPushConsumer consumer) {
        return new TracingConsumerProxyFactory(tracing, consumer).getProxy();
    }

    private TracingConsumerProxyFactory(Tracing tracing, DefaultMQPushConsumer consumer) {
        this.tracing = tracing;
        this.consumer = consumer;
    }

    private DefaultMQPushConsumer getProxy() {
        // 1. 创建Enhancer类对象，它类似于咱们JDK动态代理中的Proxy类，该类就是用来获取代理对象的
        Enhancer enhancer = new Enhancer();
        // 2. 设置父类的字节码对象。为啥子要这样做呢？因为使用CGLIB生成的代理类是属于目标类的子类的，也就是说代理类是要继承自目标类的
        enhancer.setSuperclass(consumer.getClass());
        // 3. 设置回调函数
        enhancer.setCallback(this);
        // 4. 创建代理对象
        return (DefaultMQPushConsumer) enhancer.create();
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        if (isTargetMethod(method)) {
            if (args[0] instanceof MessageListenerConcurrently) {
                return method.invoke(consumer, new TracingMessageListenerConcurrently(tracing, (MessageListenerConcurrently) args[0]));
            } else {
                return method.invoke(consumer, new TracingMessageListenerOrderly(tracing, (TracingMessageListenerOrderly) args[0]));
            }
        } else {
            return method.invoke(consumer, args);
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
            if (consumer.getConsumeMessageBatchMaxSize() > 1) {
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
