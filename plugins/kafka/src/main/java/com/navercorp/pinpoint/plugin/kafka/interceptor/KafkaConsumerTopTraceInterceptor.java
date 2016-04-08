package com.navercorp.pinpoint.plugin.kafka.interceptor;

import com.navercorp.pinpoint.bootstrap.context.MethodDescriptor;
import com.navercorp.pinpoint.bootstrap.context.SpanRecorder;
import com.navercorp.pinpoint.bootstrap.context.Trace;
import com.navercorp.pinpoint.bootstrap.context.TraceContext;
import com.navercorp.pinpoint.bootstrap.interceptor.SpanSimpleAroundInterceptor;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.plugin.kafka.KafkaPluginConstants;

/**
 * Created by jack on 4/8/16.
 */
public class KafkaConsumerTopTraceInterceptor extends SpanSimpleAroundInterceptor {
    private PLogger logger = PLoggerFactory.getLogger(this.getClass());
    private final boolean isDebug = logger.isDebugEnabled();
    private final boolean isTrace = logger.isTraceEnabled();

    public KafkaConsumerTopTraceInterceptor(TraceContext traceContext, MethodDescriptor descriptor) {
        super(traceContext, descriptor, KafkaConsumerTopTraceInterceptor.class);
    }

    @Override
    protected Trace createTrace(Object target, Object[] args) {
        logger.error("KafkaConsumerTopTraceInterceptor.createTrace:   enter .......... create trace");
        return traceContext.newTraceObject();
    }
    @Override
    protected void doInBeforeTrace(SpanRecorder recorder, Object target, Object[] args) {
        if (isDebug) {
            logger.beforeInterceptor(target, args);
        }
        logger.error("KafkaConsumerTopTraceInterceptor.doInBeforeTrace:   enter .......... ");
        recorder.recordServiceType(KafkaPluginConstants.KAFKA_SERVICE_TYPE);
    }

    @Override
    protected void doInAfterTrace(SpanRecorder recorder, Object target, Object[] args, Object result, Throwable throwable) {
        recorder.recordApi(methodDescriptor);
        if (throwable != null)
            recorder.recordException(throwable);
    }
}
