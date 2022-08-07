package top.liumian.zipkin.util;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author liumian  2022/8/6 13:15
 */
public class TraceUtil {


    /**
     * 开启一个新的链路，适用于需要返回业务逻辑执行结果的场景
     *
     * @param tracing    tracing
     * @param tranceName 链路名称
     * @param function   自定义业务逻辑
     * @param <R>        返回类型
     * @return 业务逻辑执行结果
     */
    public static <R> R newTrace(Tracing tracing, String tranceName, Function<Span, R> function) {
        Tracer tracer = tracing.tracer();
        Span span = tracer.newTrace().name(tranceName).start();
        span.annotate(tranceName + ".start");
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            return function.apply(span);
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.tag("thread", Thread.currentThread().getName());
            span.annotate(tranceName + ".finish");
            span.finish();
        }
    }

    /**
     * 开启一个新的链路，适用于不需要返回业务逻辑执行结果的场景
     *
     * @param tracing    tracing
     * @param tranceName 链路名称
     * @param consumer   自定义业务逻辑
     */
    public static void newTrace(Tracing tracing, String tranceName, Consumer<Span> consumer) {
        Tracer tracer = tracing.tracer();
        Span span = tracer.newTrace().name(tranceName).start();
        span.annotate(tranceName + ".start");
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            consumer.accept(span);
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.tag("thread", Thread.currentThread().getName());
            span.annotate(tranceName + ".finish");
            span.finish();
        }
    }


    /**
     * 开启一条子链路，适用于需要返回业务逻辑执行结果的场景
     *
     * @param tracing   tracing
     * @param traceName 链路名称
     * @param function  自定义业务逻辑
     * @param <R>       返回类型
     * @return 业务逻辑返回结果
     */
    public static <R> R newChildTrace(Tracing tracing, String traceName, Function<Span, R> function) {
        Tracer tracer = tracing.tracer();
        Span span = tracer.nextSpan().name(traceName).start();
        span.annotate(traceName + ".start");
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            return function.apply(span);
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.tag("thread", Thread.currentThread().getName());
            span.annotate(traceName + ".finish");
            span.finish();
        }
    }


    /**
     * 开启一条子链路，适用于需要返回业务逻辑执行结果的场景
     *
     * @param tracing   tracing
     * @param traceName 链路名称
     * @param consumer  自定义业务逻辑
     */
    public static void newChildTrace(Tracing tracing, String traceName, Consumer<Span> consumer) {
        Tracer tracer = tracing.tracer();
        Span span = tracer.nextSpan().name(traceName).start();
        span.annotate(traceName + ".start");
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            consumer.accept(span);
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.tag("thread", Thread.currentThread().getName());
            span.annotate(traceName + ".finish");
            span.finish();
        }
    }

    /**
     * 将properties中的链路上下文提取出来并注入到内存中
     *
     * @param tracing    tracing
     * @param traceName  链路名称
     * @param properties 链路上下文信息
     * @param function   自定义业务逻辑
     * @param <R>        返回类型
     * @return 自定义业务逻辑返回结果
     */
    public static <R> R extractTraceInfo(Tracing tracing, String traceName, Map<String,String> properties, Function<Span, R> function) {
        TraceContext.Extractor<Map<String,String>> extractor = tracing.propagation().extractor(Map::get);
        Tracer tracer = tracing.tracer();
        Span span;
        if (properties == null || properties.isEmpty()) {
            span = tracer.newTrace().name(traceName).start();
        } else {
            TraceContextOrSamplingFlags traceInfo = extractor.extract(properties);
            if (traceInfo != null && traceInfo.context() != null) {
                span = tracer.newChild(traceInfo.context()).name(traceName).start();
            } else {
                span = tracer.newTrace().name(traceName).start();
            }
        }
        span.kind(Span.Kind.CONSUMER);
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            return function.apply(span);
        } catch (Exception e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
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
    public static <R> R injectTraceInfo(Tracing tracing, String traceName, TracingFunction<Map<String, String>, R> function) throws Throwable {
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
