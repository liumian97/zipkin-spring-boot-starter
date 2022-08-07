package top.liumian.zipkin.core.mq.producer;

/**
 * @author liumian  2022/8/6 15:49
 */
@FunctionalInterface
public interface SendMessageFunction<T, R> {

    R apply(T t) throws Throwable;

}
