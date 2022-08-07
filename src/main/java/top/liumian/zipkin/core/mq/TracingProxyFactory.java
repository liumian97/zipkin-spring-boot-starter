package top.liumian.zipkin.core.mq;

import brave.Tracing;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;

/**
 * @author liumian  2022/8/7 17:08
 */
public abstract class TracingProxyFactory<T> implements MethodInterceptor {

    protected final Tracing tracing;
    protected final T instance;

    public TracingProxyFactory(Tracing tracing, T instance) {
        this.tracing = tracing;
        this.instance = instance;
    }

    /**
     * 创建代理
     *
     * @return 代理对象
     */
    protected T getProxy() {
        // 1. 创建Enhancer类对象，它类似于咱们JDK动态代理中的Proxy类，该类就是用来获取代理对象的
        Enhancer enhancer = new Enhancer();
        // 2. 设置父类的字节码对象。为啥子要这样做呢？因为使用CGLIB生成的代理类是属于目标类的子类的，也就是说代理类是要继承自目标类的
        enhancer.setSuperclass(instance.getClass());
        // 3. 设置回调函数
        enhancer.setCallback(this);
        // 4. 创建代理对象
        return (T) enhancer.create();
    }

}
