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

import com.navercorp.pinpoint.bootstrap.interceptor.AroundInterceptor;
import com.navercorp.pinpoint.bootstrap.interceptor.annotation.IgnoreMethod;
import com.navercorp.pinpoint.bootstrap.interceptor.annotation.TargetMethod;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.BindValueAccessor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.DatabaseInfoAccessor;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * @author Jiaqi Feng
 */
@TargetMethod(name="bind", paramTypes = { "java.lang.Object[]" })
public class CassandraBoundStatementBindInterceptor implements AroundInterceptor {

    private final PLogger logger = PLoggerFactory.getLogger(this.getClass());
    private final boolean isDebug = logger.isDebugEnabled();

    @Override
    public void before(Object target, Object[] args) {
        if (isDebug) {
            logger.beforeInterceptor(target, args);
        }
        //debug: logger.error("fengjiaqi: CassandraBoundStatementBindInterceptor: target="+target.getClass().getName());
        //debug: logger.error("fengjiaqi: CassandraBoundStatementBindInterceptor: "+(args == null? "args=null":"args.length="+args.length));
        //debug: if (args != null) logger.error("fengjiaqi: CassandraBoundStatementBindInterceptor: "+(args[0] == null? "args[0]=null":"args[0]="+args[0].getClass().getName()));
        // In case of close, we have to delete data even if the invocation failed.
        if (target instanceof BoundStatement) {
            if (args != null) {
                if (args[0] != null && args[0] instanceof Object[]) {
                    Object[] values=(Object[])args[0];
                    //debug: logger.error("fengjiaqi: CassandraBoundStatementBindInterceptor: values.length="+values.length);
                    HashMap<Integer, String> bindValue = new HashMap<Integer, String>(values.length);

                    for (int i=0; i<values.length; i++) {
                        if (values[i] != null) {
                            //debug: logger.error("fengjiaqi: CassandraBoundStatementBindInterceptor: args["+i+"]="+values[i].getClass().getName());
                            if (values[i] instanceof String) {
                                bindValue.put(i+1, (String)values[i]);
                                //debug: logger.error("fengjiaqi: CassandraBoundStatementBindInterceptor: put args["+i+"]="+values[i]);
                            } else if (values[i] instanceof Date) {
                                bindValue.put(i+1, values[i].toString());
                            } else if (values[i] instanceof Integer) {
                                bindValue.put(i+1, values[i].toString());
                            }
                        } else {
                            bindValue.put(i, "Unkn-Type");
                        }
                    }

                    PreparedStatement preparedStatement=((BoundStatement) target).preparedStatement();
                    //debug: logger.error("fengjiaqi: CassandraBoundStatementBindInterceptor: preparedStatement="+(preparedStatement==null? "null":preparedStatement.getClass().getName()));
                    if (preparedStatement instanceof BindValueAccessor) {
                        ((BindValueAccessor) preparedStatement)._$PINPOINT$_setBindValue(bindValue);
                    }
                }
            }
        }
    }

    @IgnoreMethod
    @Override
    public void after(Object target, Object[] args, Object result, Throwable throwable) {

    }
}
