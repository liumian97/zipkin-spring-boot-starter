package top.liumian.zipkin.core.mq.consumer;

import brave.Tracing;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import top.liumian.zipkin.util.TraceUtil;

import java.util.Collections;
import java.util.List;

/**
 * @author liumian  2022/8/6 14:59
 */
public class TracingMessageListenerConcurrently implements MessageListenerConcurrently {

    private final Tracing tracing;

    private final MessageListenerConcurrently messageListenerConcurrently;

    public TracingMessageListenerConcurrently(Tracing tracing, MessageListenerConcurrently messageListenerConcurrently) {
        this.tracing = tracing;
        this.messageListenerConcurrently = messageListenerConcurrently;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        MessageExt messageExt = msgs.get(0);
        return TraceUtil.extractTraceInfo(tracing, "MQ/CONSUME", messageExt.getProperties(), span -> messageListenerConcurrently.consumeMessage(Collections.singletonList(messageExt),context));
    }
}
