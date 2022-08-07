package top.liumian.zipkin.util;

/**
 * @author liumian  2022/8/6 15:49
 */
@FunctionalInterface
public interface TracingFunction<T, R> {

    /**
     * 自定义函数
     *
     * @param t 输入
     * @return 输出
     * @throws Throwable 异常
     */
    R apply(T t) throws Throwable;

}
