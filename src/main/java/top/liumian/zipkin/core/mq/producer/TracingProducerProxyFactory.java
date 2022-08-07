package top.liumian.zipkin.core.mq.producer;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.TraceContext;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author liumian  2022/8/7 11:17
 */
public class TracingProducerProxyFactory implements MethodInterceptor {

    private final Tracing tracing;

    private final DefaultMQProducer producer;

    public static DefaultMQProducer createTracingProducer(Tracing tracing, DefaultMQProducer producer) {
        return new TracingProducerProxyFactory(tracing, producer).getProxy();
    }

    private TracingProducerProxyFactory(Tracing tracing, DefaultMQProducer producer) {
        this.tracing = tracing;
        this.producer = producer;
    }

    private DefaultMQProducer getProxy() {
        // 1. 创建Enhancer类对象，它类似于咱们JDK动态代理中的Proxy类，该类就是用来获取代理对象的
        Enhancer enhancer = new Enhancer();
        // 2. 设置父类的字节码对象。为啥子要这样做呢？因为使用CGLIB生成的代理类是属于目标类的子类的，也就是说代理类是要继承自目标类的
        enhancer.setSuperclass(producer.getClass());
        // 3. 设置回调函数
        enhancer.setCallback(this);
        // 4. 创建代理对象
        return (DefaultMQProducer) enhancer.create();
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        if (isSendMessageMethod(method, args)) {
            return traceMessage((Message) args[0], message -> method.invoke(producer, args));
        } else if (isSendBatchMessageMethod(method, args)) {
            return traceMessage((Collection<Message>) args[0], messages -> method.invoke(producer, args));
        } else {
            return method.invoke(producer, args);
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

    private <R> R traceMessage(Message message, SendMessageFunction<Message, R> function) throws Throwable {
        return TracingProducerProxyFactory.injectTraceInfo(tracing, "MQ/SEND", traceInfo -> {
            Span span = tracing.tracer().currentSpan();
            traceInfo.forEach(message::putUserProperty);
            span.tag("mq.topic", message.getTopic());
            return function.apply(message);
        });
    }

    private <R> R traceMessage(Collection<Message> messageCollection, SendMessageFunction<Collection<Message>, R> function) throws Throwable {
        return TracingProducerProxyFactory.injectTraceInfo(tracing, "MQ/SEND", traceInfo -> {
            messageCollection.forEach(message -> traceInfo.forEach(message::putUserProperty));
            return function.apply(messageCollection);
        });
    }


    /**
     * 将当前链路上下文注入到properties中
     *
     * @param tracing   tracing
     * @param traceName 链路名称
     * @param function  自定义业务逻辑
     * @param <R>       返回类型
     * @return 自定义业务逻辑返回结果
     */
    public static <R> R injectTraceInfo(Tracing tracing, String traceName, SendMessageFunction<Map<String, String>, R> function) throws Throwable {
        Tracer tracer = tracing.tracer();
        Span span = tracer.nextSpan().name(traceName).start();
        TraceContext.Injector<Map<String, String>> injector = tracing.propagation().injector(Map::put);
        Map<String, String> traceInfo = new HashMap<>();
        injector.inject(span.context(), traceInfo);
        span.kind(Span.Kind.PRODUCER);
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            return function.apply(traceInfo);
        } finally {
            span.finish();
        }
    }

}
