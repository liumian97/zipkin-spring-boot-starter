package top.liumian.zipkin.core.mq;

import brave.Tracing;
import org.junit.Before;

import static brave.handler.SpanHandler.NOOP;

/**
 * @author liumian  2022/8/7 12:19
 */
public class BaseTracingTest {

    protected Tracing tracing;


    @Before
    public void initTracing() {

        tracing = Tracing.newBuilder().localServiceName("tracingTest")
                .addSpanHandler(NOOP)
//                .currentTraceContext(ThreadLocalCurrentTraceContext.newBuilder().addScopeDecorator(MDCScopeDecorator.get()).build())
                .build();
    }

}
