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

        Trace trace = traceContext.currentTraceObject();
        if (trace == null) {
            if (target instanceof KafkaHeaderSetter) {
                ((KafkaHeaderSetter) target)._$PINPOINT$_setHeader("no trace header");
            }
            return;
        }

        SpanEventRecorder recorder = trace.traceBlockBegin();
        // generate next trace id.
        final TraceId nextId = trace.getTraceId().getNextTraceId();
        recorder.recordNextSpanId(nextId.getSpanId());
        recorder.recordServiceType(KafkaPluginConstants.KAFKA_SERVICE_TYPE);

        recorder.recordApi(descriptor);

        // let's set pinpoint header to encoder

        // trace id, this contains 3 field separated by ^
        StringBuilder sb=new StringBuilder(nextId.getTransactionId());sb.append("^");
        // span id
        sb.append(String.valueOf((nextId.getSpanId())));sb.append("^");
        // parent span id
        sb.append(String.valueOf(nextId.getParentSpanId()));sb.append("^");
        // flasgs
        sb.append(String.valueOf(nextId.getFlags()));sb.append("^");
        // parrent app name
        sb.append(traceContext.getApplicationName());sb.append("^");
        //parent app type
        sb.append(Short.toString(traceContext.getServerTypeCode()));
        // host ???

        if (target instanceof KafkaHeaderSetter) {
            ((KafkaHeaderSetter) target)._$PINPOINT$_setHeader(sb.toString());
        }
    }

    @Override
    public void after(Object target, Object[] args, Object result, Throwable throwable) {
        Trace trace = traceContext.currentTraceObject();
        if (trace == null) {
            return;
        }

        try {
            if (throwable != null) {
                SpanEventRecorder recorder = trace.currentSpanEventRecorder();
                recorder.recordException(throwable);
            }
        } finally {
            trace.traceBlockEnd();
        }
    }
}
