package com.navercorp.pinpoint.plugin.rabbitmq.interceptor;

import com.navercorp.pinpoint.bootstrap.context.MethodDescriptor;
import com.navercorp.pinpoint.bootstrap.context.SpanRecorder;
import com.navercorp.pinpoint.bootstrap.context.TraceContext;
import com.navercorp.pinpoint.plugin.rabbitmq.RabbitMQConstants;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.springframework.amqp.core.Address;

import java.util.Map;

/**
 * @author Jiaqi Feng
 */
public class SpringListenerInterceptor extends RabbitMQConsumeInterceptor {
    public SpringListenerInterceptor(TraceContext traceContext, MethodDescriptor descriptor) {
        super(traceContext, descriptor);
    }

    @Override
    protected Map<String, Object> getHeaders(Object target, Object[] args) {
        if (args[1] instanceof org.springframework.amqp.core.Message) {
            org.springframework.amqp.core.Message message = (org.springframework.amqp.core.Message) args[1];
            return message.getMessageProperties().getHeaders();
        }
        return null;
    }

    @Override
    protected String getExchange(Object target, Object[] args) {
        if (args[1] instanceof org.springframework.amqp.core.Message) {
            org.springframework.amqp.core.Message message = (org.springframework.amqp.core.Message) args[1];
            return message.getMessageProperties().getReceivedExchange();
        }
        return null;
    }

    @Override
    protected String getRoutingKey(Object target, Object[] args) {
        if (args[1] instanceof org.springframework.amqp.core.Message) {
            org.springframework.amqp.core.Message message = (org.springframework.amqp.core.Message) args[1];
            return message.getMessageProperties().getReceivedRoutingKey();
        }
        return null;
    }

    @Override
    protected String getChannelAddress(Object target, Object[] args) {
        if (args[1] instanceof com.rabbitmq.client.Channel) {
            com.rabbitmq.client.Channel channel = (com.rabbitmq.client.Channel) args[0];
            Connection connection = channel.getConnection();
            return connection.getAddress().getHostAddress() + ":" + connection.getPort();
        }
        return null;
    }

    @Override
    protected void doInAfterTrace(SpanRecorder recorder, Object target, Object[] args, Object result, Throwable throwable) {
        String exchange=getExchange(target, args);
        if (exchange == null || exchange.equals(""))
            exchange = "unknown";

        recorder.recordApi(methodDescriptor);
        recorder.recordAttribute(RabbitMQConstants.RABBITMQ_ROUTINGKEY_ANNOTATION_KEY, getRoutingKey(target, args));
        if (args[0] instanceof com.rabbitmq.client.Channel) {
            com.rabbitmq.client.Channel channel = (com.rabbitmq.client.Channel) args[0];
            Connection connection = channel.getConnection();
            recorder.recordRemoteAddress(connection.getAddress().getHostAddress() + ":" + connection.getPort());
        }

        if (throwable != null) {
            recorder.recordException(throwable);
        }
    }
}
