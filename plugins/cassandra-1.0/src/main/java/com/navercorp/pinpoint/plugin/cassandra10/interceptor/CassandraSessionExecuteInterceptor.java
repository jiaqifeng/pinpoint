/*
 * Copyright 2014 NAVER Corp.
 *
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

package com.navercorp.pinpoint.plugin.cassandra10.interceptor;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.navercorp.pinpoint.bootstrap.context.DatabaseInfo;
import com.navercorp.pinpoint.bootstrap.context.MethodDescriptor;
import com.navercorp.pinpoint.bootstrap.context.ParsingResult;
import com.navercorp.pinpoint.bootstrap.context.SpanEventRecorder;
import com.navercorp.pinpoint.bootstrap.context.Trace;
import com.navercorp.pinpoint.bootstrap.context.TraceContext;
import com.navercorp.pinpoint.bootstrap.interceptor.AroundInterceptor;
import com.navercorp.pinpoint.bootstrap.interceptor.annotation.TargetMethod;
import com.navercorp.pinpoint.bootstrap.interceptor.annotation.TargetMethods;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.BindValueAccessor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.DatabaseInfoAccessor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.ParsingResultAccessor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.UnKnownDatabaseInfo;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.bindvalue.BindValueUtils;

/**
 * @author dawidmalina
 */
@TargetMethods({
    @TargetMethod(name = "execute", paramTypes = { "java.lang.String" }),
    @TargetMethod(name = "execute", paramTypes = { "com.datastax.driver.core.Query" })
})
public class CassandraSessionExecuteInterceptor implements AroundInterceptor {

    private static final int DEFAULT_BIND_VALUE_LENGTH = 1024;

    private final PLogger logger = PLoggerFactory.getLogger(this.getClass());
    private final boolean isDebug = logger.isDebugEnabled();

    private final MethodDescriptor descriptor;
    private final TraceContext traceContext;
    private final int maxSqlBindValueLength;

    public CassandraSessionExecuteInterceptor(TraceContext traceContext, MethodDescriptor descriptor) {
        this(traceContext, descriptor, DEFAULT_BIND_VALUE_LENGTH);
    }

    public CassandraSessionExecuteInterceptor(TraceContext traceContext, MethodDescriptor descriptor,
            int maxSqlBindValueLength) {
        this.traceContext = traceContext;
        this.descriptor = descriptor;
        this.maxSqlBindValueLength = maxSqlBindValueLength;
    }

    @Override
    public void before(Object target, Object[] args) {
        if (isDebug) {
            logger.beforeInterceptor(target, args);
        }

        Trace trace = traceContext.currentTraceObject();
        if (trace == null) {
            return;
        }

        SpanEventRecorder recorder = trace.traceBlockBegin();
        try {
            DatabaseInfo databaseInfo = (target instanceof DatabaseInfoAccessor)
                    ? ((DatabaseInfoAccessor) target)._$PINPOINT$_getDatabaseInfo() : null;

            if (databaseInfo == null) {
                databaseInfo = UnKnownDatabaseInfo.INSTANCE;
            }

            recorder.recordServiceType(databaseInfo.getExecuteQueryType());
            recorder.recordEndPoint(databaseInfo.getMultipleHost());
            recorder.recordDestinationId(databaseInfo.getDatabaseId());

            String sql;
            if (args[0] instanceof BoundStatement) {
                sql = ((BoundStatement) args[0]).preparedStatement().getQueryString();
                //debug: logger.error("fengjiaqi: args[0] is BoundStatement, sql="+sql);
            } else if (args[0] instanceof Statement) {
                sql = ((Statement) args[0]).getQueryString();
                //debug: logger.error("fengjiaqi: args[0] is Statement, sql="+sql);
            } else {
                // we have string TODO I hope so!
                sql = (String) args[0];
                //debug: logger.error("fengjiaqi: args[0] is String, sql="+sql);
            }

            ParsingResult parsingResult = traceContext.parseSql(sql);
            if (parsingResult != null) {
                ((ParsingResultAccessor) target)._$PINPOINT$_setParsingResult(parsingResult);
            } else {
                if (logger.isErrorEnabled()) {
                    logger.error("sqlParsing fail. parsingResult is null sql:{}", sql);
                }
            }

            Map<Integer, String> bindValue=null;
            if (args != null) {
                //debug: logger.error("fengjiaqi: args!=null, type="+args.getClass().getName());
                if (args instanceof Object[]) {
                    //debug: logger.error("fengjiaqi: args is Object[]");
                    if (args[0] != null) {
                        //debug: logger.error("fengjiaqi: args[0] type="+args[0].getClass().getName());
                    }
                }
            }
            if (args !=null && args[0] != null && args[0] instanceof BoundStatement) {
                PreparedStatement preparedStatement=((BoundStatement)args[0]).preparedStatement();
                bindValue = ((BindValueAccessor) preparedStatement)._$PINPOINT$_getBindValue();
            }
            // TODO Add bind variable interceptors to BoundStatement's setter methods and bind method and pass it down
            // Extracting bind variables from already-serialized is too risky
            if (bindValue != null && !bindValue.isEmpty()) {
                //debug: logger.error("fengjiaqi: bindValue not null, length="+bindValue.size());
                String bindString = toBindVariable(bindValue);
                //debug: logger.error("fengjiaqi: bindString ="+bindString);
                recorder.recordSqlParsingResult(parsingResult, bindString);
            } else {
                //debug: logger.error("fengjiaqi: bindString not exist");
                recorder.recordSqlParsingResult(parsingResult);
            }

            recorder.recordApi(descriptor);
            clean(target);

        } catch (Exception e) {
            if (logger.isWarnEnabled()) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    private void clean(Object target) {
        if (target instanceof BindValueAccessor) {
            ((BindValueAccessor) target)._$PINPOINT$_setBindValue(new HashMap<Integer, String>());
        }
    }

    private String toBindVariable(Map<Integer, String> bindValue) {
        return BindValueUtils.bindValueToString(bindValue, maxSqlBindValueLength);
    }

    @Override
    public void after(Object target, Object[] args, Object result, Throwable throwable) {
        if (isDebug) {
            logger.afterInterceptor(target, args, result, throwable);
        }

        Trace trace = traceContext.currentTraceObject();
        if (trace == null) {
            return;
        }

        try {
            SpanEventRecorder recorder = trace.currentSpanEventRecorder();
            // TODO Test if it's success. if failed terminate. else calculate
            // resultset fetch too. we'd better make resultset fetch optional.
            recorder.recordException(throwable);
        } finally {
            trace.traceBlockEnd();
        }
    }
}
