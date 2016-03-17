/*
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
package com.navercorp.pinpoint.plugin.cassandra10;

import static com.navercorp.pinpoint.common.util.VarArgs.va;

import java.security.ProtectionDomain;

import com.navercorp.pinpoint.bootstrap.instrument.InstrumentClass;
import com.navercorp.pinpoint.bootstrap.instrument.InstrumentException;
import com.navercorp.pinpoint.bootstrap.instrument.Instrumentor;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformCallback;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformTemplate;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformTemplateAware;
import com.navercorp.pinpoint.bootstrap.interceptor.scope.ExecutionPolicy;
import com.navercorp.pinpoint.bootstrap.plugin.ProfilerPlugin;
import com.navercorp.pinpoint.bootstrap.plugin.ProfilerPluginSetupContext;

/**
 * @author dawidmalina
 */
public class CassandraPlugin implements ProfilerPlugin, TransformTemplateAware {

    // AbstractSession got added in 1.0.5
    private static final String CLASS_SESSION = "com.datastax.driver.core.Session";

    private TransformTemplate transformTemplate;

    @Override
    public void setup(ProfilerPluginSetupContext context) {
        CassandraConfig config = new CassandraConfig(context.getConfig());

        if (config.isCassandra()) {
            addDefaultPreparedStatementTransformer();
            addSessionTransformer(config);
            addClusterTransformer();
        }
    }

    private void addDefaultPreparedStatementTransformer() {
        TransformCallback transformer = new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className,
                    Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer)
                            throws InstrumentException {

                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                if (!target.isInterceptable()) {
                    return null;
                }

                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.DatabaseInfoAccessor");
                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.ParsingResultAccessor");
                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.BindValueAccessor");

                return target.toBytecode();
            }
        };

        transformTemplate.transform("com.datastax.driver.core.PreparedStatement", transformer);
    }

    private void addSessionTransformer(final CassandraConfig config) {

        TransformCallback transformer = new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className,
                                        Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer)
                    throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                if (!target.isInterceptable()) {
                    return null;
                }

                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.DatabaseInfoAccessor");
                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.ParsingResultAccessor");
                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.BindValueAccessor");

                target.addScopedInterceptor(
                        "com.navercorp.pinpoint.plugin.cassandra10.interceptor.CassandraSessionShutdownInterceptor",
                        CassandraConstants.CASSANDRA_SCOPE);
                target.addScopedInterceptor(
                        "com.navercorp.pinpoint.plugin.cassandra10.interceptor.CassandraSessionPrepareInterceptor",
                        CassandraConstants.CASSANDRA_SCOPE);

                target.addScopedInterceptor(
                        "com.navercorp.pinpoint.plugin.cassandra10.interceptor.CassandraSessionExecuteInterceptor",
                        va(config.getMaxSqlBindValueSize()), CassandraConstants.CASSANDRA_SCOPE);

                return target.toBytecode();
            }
        };

        transformTemplate.transform(CLASS_SESSION, transformer);
    }

    private void addClusterTransformer() {
        transformTemplate.transform("com.datastax.driver.core.Cluster", new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className,
                    Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer)
                            throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                if (!target.isInterceptable()) {
                    return null;
                }

                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.DatabaseInfoAccessor");

                target.addScopedInterceptor(
                        "com.navercorp.pinpoint.plugin.cassandra10.interceptor.CassandraClusterConnectInterceptor",
                        va(true), CassandraConstants.CASSANDRA_SCOPE, ExecutionPolicy.ALWAYS);

                target.addScopedInterceptor(
                        "com.navercorp.pinpoint.plugin.cassandra10.interceptor.CassandraSessionShutdownInterceptor",
                        CassandraConstants.CASSANDRA_SCOPE);

                return target.toBytecode();
            }
        });
    }

    @Override
    public void setTransformTemplate(TransformTemplate transformTemplate) {
        this.transformTemplate = transformTemplate;
    }
}
