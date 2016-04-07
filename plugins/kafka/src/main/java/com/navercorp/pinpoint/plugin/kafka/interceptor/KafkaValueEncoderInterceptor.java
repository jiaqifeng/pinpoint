package com.navercorp.pinpoint.plugin.kafka.interceptor;

import com.navercorp.pinpoint.bootstrap.async.AsyncTraceIdAccessor;
import com.navercorp.pinpoint.bootstrap.context.*;
import com.navercorp.pinpoint.bootstrap.interceptor.AroundInterceptor;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.plugin.kafka.KafkaHeaderSetter;
import com.navercorp.pinpoint.plugin.kafka.KafkaPluginConstants;

/**
 * @author Jiaqi Feng
 */
public class KafkaValueEncoderInterceptor implements AroundInterceptor {
    private final PLogger logger = PLoggerFactory.getLogger(this.getClass());
    private final boolean isDebug = logger.isDebugEnabled();

    private final MethodDescriptor descriptor;
    private final TraceContext traceContext;

    public KafkaValueEncoderInterceptor(TraceContext traceContext, MethodDescriptor descriptor) {
        this.traceContext = traceContext;
        this.descriptor = descriptor;
    }

    @Override
    public void before(Object target, Object[] args) {
        if (isDebug) {
            logger.beforeInterceptor(target, args);
        }

        if (!(target instanceof KafkaHeaderSetter)) {
            logger.error("KafkaValueEncoderInterceptor.before(): target is not KafkaHeaderSetter!!! Ignored");
            return;
        }

        Trace trace = traceContext.currentTraceObject();
        if (trace == null) {
            logger.error("KafkaValueEncoderInterceptor.before(): no trace found, set dummy header and return");
            // TODO we'd better set a header, or maybe the receiver could remove it without guess
            ((KafkaHeaderSetter) target)._$PINPOINT$_setHeader("no trace header");
            return;
        }

        SpanEventRecorder recorder = trace.traceBlockBegin();
        recorder.recordServiceType(KafkaPluginConstants.KAFKA_SERVICE_TYPE);

        // generate next trace id.
        TraceId nextId = trace.getTraceId().getNextTraceId();

        recorder.recordNextSpanId(nextId.getSpanId());

        // rabbitmq do not set this
        // recorder.recordApi(descriptor);

        // let's set pinpoint header to encoder
        // trace id, this contains 3 field separated by ^
        StringBuilder sb = new StringBuilder(nextId.getTransactionId());sb.append("^");
        // parent span id
        sb.append(String.valueOf(nextId.getParentSpanId()));sb.append("^");
        // span id
        sb.append(String.valueOf((nextId.getSpanId())));sb.append("^");
        // app type
        sb.append(Short.toString(KafkaPluginConstants.KAFKA_SERVICE_TYPE.getCode()));sb.append("^");
        // parrent app name
        sb.append(traceContext.getApplicationName());sb.append("^");
        // flasgs
        sb.append(String.valueOf(nextId.getFlags()));sb.append("^");
        // host ???

        if (trace.canSampled()) {
            sb.append("0");
        } else {
            sb.append("1");
        }

        ((KafkaHeaderSetter) target)._$PINPOINT$_setHeader(sb.toString());
    }

    @Override
    public void after(Object target, Object[] args, Object result, Throwable throwable) {
        if (isDebug) {
            logger.afterInterceptor(target, args);
        }

        Trace trace = traceContext.currentTraceObject();
        if (trace == null || !trace.canSampled()) {
            return;
        }

        try {
            SpanEventRecorder recorder = trace.currentSpanEventRecorder();
            recorder.recordApi(descriptor);
            if (throwable == null) {logger.error("KafkaValueEncoderInterceptor.after(): after return normal");
                String exchange="unknown";
                recorder.recordEndPoint(exchange);
                recorder.recordDestinationId(exchange);
//                recorder.recordAttribute(RabbitMQConstants.RABBITMQ_EXCHANGE_ANNOTATION_KEY, exchange);
//                recorder.recordAttribute(RabbitMQConstants.RABBITMQ_ROUTINGKEY_ANNOTATION_KEY, routingKey);
//                recorder.recordAttribute(RabbitMQConstants.RABBITMQ_PROPERTIES_ANNOTATION_KEY, properties);
//                recorder.recordAttribute(RabbitMQConstants.RABBITMQ_BODY_ANNOTATION_KEY, body);
            } else {logger.error("KafkaValueEncoderInterceptor.after(): get throwable");
                recorder.recordException(throwable);
            }
        } finally {
            trace.traceBlockEnd();
        }
    }
}
