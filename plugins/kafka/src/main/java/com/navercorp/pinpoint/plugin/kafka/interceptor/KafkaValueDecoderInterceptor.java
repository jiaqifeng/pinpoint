/**
 * Copyright 2014 NAVER Corp.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.navercorp.pinpoint.plugin.kafka.interceptor;

import com.navercorp.pinpoint.bootstrap.context.*;
import com.navercorp.pinpoint.bootstrap.interceptor.AroundInterceptor;
import com.navercorp.pinpoint.bootstrap.interceptor.SpanSimpleAroundInterceptor;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.bootstrap.util.NumberUtils;
import com.navercorp.pinpoint.plugin.kafka.KafkaPluginConstants;

/**
 * @author Jiaqi Feng
 */

public class KafkaValueDecoderInterceptor implements AroundInterceptor {
    protected final PLogger logger;
    protected final boolean isDebug;

    protected final MethodDescriptor methodDescriptor;
    protected final TraceContext traceContext;

    public KafkaValueDecoderInterceptor(TraceContext traceContext, MethodDescriptor descriptor) {
        this.traceContext = traceContext;
        this.methodDescriptor = descriptor;
        this.logger = PLoggerFactory.getLogger(KafkaValueDecoderInterceptor.class);
        this.isDebug = logger.isDebugEnabled();
    }

    @Override
    public void before(Object target, Object[] args) {
        try { throw new Exception("jack kafka decoder interceptor");}catch(Exception e){e.printStackTrace();};
        if (isDebug) {
            logger.beforeInterceptor(target, args);
        }

        try {
            final Trace trace = createTrace(target, args);
            if (trace == null) {
                return;
            }
            // TODO STATDISABLE this logic was added to disable statistics tracing
            if (!trace.canSampled()) {
                return;
            }
            // ------------------------------------------------------
            final SpanRecorder recorder = trace.getSpanRecorder();
            doInBeforeTrace(recorder, target, args);
        } catch (Throwable th) {
            if (logger.isWarnEnabled()) {
                logger.warn("BEFORE. Caused:{}", th.getMessage(), th);
            }
        }
    }

    protected Trace createTrace(Object target, Object[] args) {
        // args[0] should be the kafka KeyedMessage.value as byt[]
        if (args == null || !(args[0] instanceof byte[])) {
            logger.error("KafkaValueDecoderInterceptor.createTrace(): args or args[0] is not what expected, return no trace");
            return null;
        }

        String value = new String((byte[]) args[0]);
        System.out.println("KafkaValueDecoderInterceptor.createTrace(): got value=" + value);
        int pos = value.indexOf("\n");
        if (pos == -1) {
            System.out.println("KafkaValueDecoderInterceptor.createTrace(): no header, return no trace");
            return null;
        }

        String[] headers = value.substring(0, pos).split("\\^");
        logger.error("KafkaValueDecoderInterceptor.createTrace(): header contains " + headers.length + " field");
        for (String s : headers)
            logger.error("KafkaValueDecoderInterceptor.createTrace(): get id=" + s);
        if (headers.length < 8) {
            System.out.println("KafkaValueDecoderInterceptor.createTrace(): header fileds not enough, return no trace");
            return null;
        }

        // exact all trace/span info
        String transactionId = headers[0] + "^" + headers[1] + "^" + headers[2];
        long parentSpanID = NumberUtils.parseLong(headers[3], SpanId.NULL);
        long spanID = NumberUtils.parseLong(headers[4], SpanId.NULL);
        short flags = NumberUtils.parseShort(headers[7], (short) 0);

        TraceId traceId = traceContext.createTraceId(transactionId, parentSpanID, spanID, flags);

        Trace trace = traceContext.currentTraceObject();
        if (trace == null) {
            logger.error("KafkaValueDecoderInterceptor.createTrace(): .......... parentSpan=" + parentSpanID + ", spanId=" + spanID + ",traceId=" + traceId);
            trace = traceContext.continueTraceObject(traceId); // this will create a new DefaultTrace and return it
        } else {
            logger.error("KafkaValueDecoderInterceptor.createTrace(): .......... merge parentSpan=" + parentSpanID + ", spanId=" + spanID + ",traceId=" + traceId);
            // there are trace already, lets replace it with new one, and create a spanevent?
            trace = traceContext.updateAsContinueTraceObject(traceId);
        }

        logger.error("fengjiaqi KafkaValueDecoderInterceptor: merged");
        return trace;
    }

    protected void doInBeforeTrace(SpanRecorder recorder, Object target, Object[] args) {
        // You have to record a service type within Server range.
        recorder.recordServiceType(KafkaPluginConstants.KAFKA_SERVICE_TYPE);
        String exchange="unknown";// TODO should be kafka hostname or ip
        if (exchange == null || exchange.equals("")) exchange = "unknown";
        // Record client address, server address.
        recorder.recordEndPoint(exchange);

        recorder.recordParentApplication(exchange, KafkaPluginConstants.KAFKA_SERVICE_TYPE.getCode());
        recorder.recordAcceptorHost(exchange);
        logger.error("fengjiaqi KafkaValueDecoderInterceptor: doInBefore endPoint="+exchange);
    }

    @Override
    public void after(Object target, Object[] args, Object result, Throwable throwable) {
        if (isDebug) {
            logger.afterInterceptor(target, args, result, throwable);
        }

        final Trace trace = traceContext.currentRawTraceObject();
        if (trace == null) {
            return;
        }

        // TODO STATDISABLE this logic was added to disable statistics tracing
        if (!trace.canSampled()) {
            traceContext.removeTraceObject();
            return;
        }
        // ------------------------------------------------------
        try {
            final SpanRecorder recorder = trace.getSpanRecorder();
            doInAfterTrace(recorder, target, args, result, throwable);
        } catch (Throwable th) {
            if (logger.isWarnEnabled()) {
                logger.warn("AFTER. Caused:{}", th.getMessage(), th);
            }
        } finally {
            traceContext.removeTraceObject();
            deleteTrace(trace, target, args, result, throwable);
        }
    }

    private void doInAfterTrace(final SpanRecorder recorder, Object target, Object[] args, Object result, Throwable throwable) {
        String header=null;

        recorder.recordApi(methodDescriptor);
        recorder.recordRemoteAddress("unknownip:port");

        if (throwable != null) {
            recorder.recordException(throwable);
        }
    }

    protected void deleteTrace(final Trace trace, final Object target, final Object[] args, final Object result, Throwable throwable) {
        trace.close();
    }
}
