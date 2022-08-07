package top.liumian.zipkin.core.mq.consumer;

import brave.Tracing;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import top.liumian.zipkin.util.TraceUtil;

import java.util.Collections;
import java.util.List;

/**
 * @author liumian  2022/8/6 14:59
 */
public class TracingMessageListenerOrderly implements MessageListenerOrderly {


    private final Tracing tracing;

    private final MessageListenerOrderly MessageListenerOrderly;

    public TracingMessageListenerOrderly(Tracing tracing, MessageListenerOrderly messageListenerOrderly) {
        this.tracing = tracing;
        this.MessageListenerOrderly = messageListenerOrderly;
    }

    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        MessageExt messageExt = msgs.get(0);
        return TraceUtil.extractTraceInfo(tracing, "MQ/CONSUME", messageExt.getProperties(), span -> MessageListenerOrderly.consumeMessage(Collections.singletonList(messageExt),context));
    }
}
