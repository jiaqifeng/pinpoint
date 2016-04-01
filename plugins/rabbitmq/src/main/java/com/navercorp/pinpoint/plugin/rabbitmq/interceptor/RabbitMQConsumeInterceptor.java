package com.navercorp.pinpoint.plugin.rabbitmq.interceptor;

import com.navercorp.pinpoint.bootstrap.context.*;
import com.navercorp.pinpoint.bootstrap.interceptor.SpanSimpleAroundInterceptor;
import com.navercorp.pinpoint.bootstrap.util.NumberUtils;
import com.navercorp.pinpoint.plugin.rabbitmq.RabbitMQConstants;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.util.Map;

/**
 * @author Jinkai.Ma
 */
public class RabbitMQConsumeInterceptor extends SpanSimpleAroundInterceptor {

    public RabbitMQConsumeInterceptor(TraceContext traceContext, MethodDescriptor descriptor) {
        super(traceContext, descriptor, RabbitMQConsumeInterceptor.class);
    }

    @Override
    protected Trace createTrace(Object target, Object[] args) {
        AMQP.BasicProperties properties = (AMQP.BasicProperties) args[2];
        Map<String, Object> headers = properties.getHeaders();
        // If this transaction is not traceable, mark as disabled.
        if (headers.get(RabbitMQConstants.META_DO_NOT_TRACE) != null) {logger.error("fengjiaqi RabbitMQConsumeInterceptor: createTrace: disableSampling");
            return traceContext.disableSampling();
        }

        Object transactionId = headers.get(RabbitMQConstants.META_TRANSACTION_ID);
        // If there's no trasanction id, a new trasaction begins here.
        if (transactionId == null) {logger.error("fengjiaqi RabbitMQConsumeInterceptor: createTrace: transactionId = null");
            return traceContext.newTraceObject();
        }

        // otherwise, continue tracing with given data.
        long parentSpanID = NumberUtils.parseLong(headers.get(RabbitMQConstants.META_PARENT_SPAN_ID).toString(), SpanId.NULL);
        long spanID = NumberUtils.parseLong(headers.get(RabbitMQConstants.META_SPAN_ID).toString(), SpanId.NULL);
        short flags = NumberUtils.parseShort(headers.get(RabbitMQConstants.META_FLAGS).toString(), (short) 0);
        TraceId traceId = traceContext.createTraceId(transactionId.toString(), parentSpanID, spanID, flags);
        logger.error("fengjiaqi RabbitMQConsumeInterceptor: createTrace: parentSpan="+parentSpanID+", spanId="+spanID+",traceId="+traceId);
        return traceContext.continueTraceObject(traceId);
    }


    @Override
    protected void doInBeforeTrace(SpanRecorder recorder, Object target, Object[] args) {
        Envelope envelope = (Envelope) args[1];

        // You have to record a service type within Server range.
        recorder.recordServiceType(RabbitMQConstants.RABBITMQ_SERVICE_TYPE);
        String exchange=envelope.getExchange();
        if (exchange == null || exchange.equals("")) exchange = "unknown";
        // Record client address, server address.
        recorder.recordEndPoint(exchange);

        recorder.recordParentApplication(exchange, RabbitMQConstants.RABBITMQ_SERVICE_TYPE.getCode());
        recorder.recordAcceptorHost(exchange);
        logger.error("fengjiaqi RabbitMQConsumeInterceptor: doInBefore endPoint="+envelope.getExchange()+",="+exchange);
    }

    @Override
    protected void doInAfterTrace(SpanRecorder recorder, Object target, Object[] args, Object result, Throwable throwable) {
        DefaultConsumer consumer = (DefaultConsumer) target;
        Connection connection = consumer.getChannel().getConnection();
        Envelope envelope = (Envelope) args[1];
        AMQP.BasicProperties properties = (AMQP.BasicProperties) args[2];
        byte[] body = (byte[]) args[3];

        String exchange=envelope.getExchange();
        if (exchange == null || exchange.equals("")) exchange = "unknown";

        recorder.recordApi(methodDescriptor);
        recorder.recordAttribute(RabbitMQConstants.RABBITMQ_EXCHANGE_ANNOTATION_KEY, exchange);
        recorder.recordAttribute(RabbitMQConstants.RABBITMQ_ROUTINGKEY_ANNOTATION_KEY, envelope.getRoutingKey());
        recorder.recordAttribute(RabbitMQConstants.RABBITMQ_PROPERTIES_ANNOTATION_KEY, properties);
        recorder.recordAttribute(RabbitMQConstants.RABBITMQ_BODY_ANNOTATION_KEY, body);
        recorder.recordRemoteAddress(connection.getAddress().getHostAddress() + ":" + connection.getPort());
        logger.error("fengjiaqi RabbitMQConsumeInterceptor: routeKey="+envelope.getRoutingKey());
        logger.error("fengjiaqi RabbitMQConsumeInterceptor: body="+(new String(body)));
        logger.error("fengjiaqi RabbitMQConsumeInterceptor: host="+connection.getAddress().getHostAddress());
        logger.error("fengjiaqi RabbitMQConsumeInterceptor: port="+connection.getPort());
        logger.error("fengjiaqi RabbitMQConsumeInterceptor: exchange="+envelope.getExchange()+",="+exchange);
        if (throwable != null) {
            recorder.recordException(throwable);
        }
    }

}
