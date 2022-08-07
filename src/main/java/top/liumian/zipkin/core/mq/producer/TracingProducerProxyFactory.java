package top.liumian.zipkin.core.mq.producer;

import brave.Span;
import brave.Tracing;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import top.liumian.zipkin.core.mq.TracingProxyFactory;
import top.liumian.zipkin.util.TraceUtil;
import top.liumian.zipkin.util.TracingFunction;

import java.lang.reflect.Method;
import java.util.Collection;

/**
 * @author liumian  2022/8/7 11:17
 */
public class TracingProducerProxyFactory extends TracingProxyFactory<DefaultMQProducer> {

    public static DefaultMQProducer createTracingProducer(Tracing tracing, DefaultMQProducer producer) {
        return new TracingProducerProxyFactory(tracing, producer).getProxy();
    }

    private TracingProducerProxyFactory(Tracing tracing, DefaultMQProducer producer) {
        super(tracing,producer);
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        if (isSendMessageMethod(method, args)) {
            return traceMessage((Message) args[0], message -> method.invoke(instance, args));
        } else if (isSendBatchMessageMethod(method, args)) {
            return traceMessage((Collection<Message>) args[0], messages -> method.invoke(instance, args));
        } else {
            return method.invoke(instance, args);
        }
    }

    /**
     * 判断是否是发送消息方法
     *
     * @param method 当前调用方法
     * @return 判断结果
     */
    private boolean isSendMessageMethod(Method method, Object[] args) {
        String methodName = method.getName();
        if (methodName.startsWith("send") || methodName.startsWith("request")) {
            return args[0] instanceof Message;
        } else {
            return false;
        }
    }


    /**
     * 判断是否是发送消息方法
     *
     * @param method 当前调用方法
     * @return 判断结果
     */
    private boolean isSendBatchMessageMethod(Method method, Object[] args) {
        String methodName = method.getName();
        if (methodName.startsWith("send") || methodName.startsWith("request")) {
            return args[0] instanceof Collection;
        } else {
            return false;
        }
    }

    /**
     * 将当前链路信息注入到message中
     *
     * @param message  MQ业务消息
     * @param function 业务方法
     * @param <R>      泛型
     * @return 业务方法执行结果
     * @throws Throwable 异常
     */
    private <R> R traceMessage(Message message, TracingFunction<Message, R> function) throws Throwable {
        return TraceUtil.injectTraceInfo(tracing, "MQ/SEND", traceInfo -> {
            Span span = tracing.tracer().currentSpan();
            traceInfo.forEach(message::putUserProperty);
            span.tag("mq.topic", message.getTopic());
            return function.apply(message);
        });
    }

    /**
     * 将当前链路信息注入到message中
     *
     * @param messageCollection MQ业务消息集合
     * @param function          业务方法
     * @param <R>               泛型
     * @return 业务方法执行结果
     * @throws Throwable 异常
     */
    private <R> R traceMessage(Collection<Message> messageCollection, TracingFunction<Collection<Message>, R> function) throws Throwable {
        return TraceUtil.injectTraceInfo(tracing, "MQ/SEND", traceInfo -> {
            messageCollection.forEach(message -> traceInfo.forEach(message::putUserProperty));
            return function.apply(messageCollection);
        });
    }


}
