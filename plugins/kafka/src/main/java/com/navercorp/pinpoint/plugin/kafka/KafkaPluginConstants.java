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
package com.navercorp.pinpoint.plugin.kafka;

import com.navercorp.pinpoint.common.trace.AnnotationKey;
import com.navercorp.pinpoint.common.trace.AnnotationKeyFactory;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.common.trace.ServiceTypeFactory;

/**
 * @author Jiaqi Feng
 *
 */
public interface KafkaPluginConstants {
    public static final ServiceType KAFKA_SERVICE_TYPE = ServiceTypeFactory.of(9120+3, "KAFKA_COMMAND");

    public static final String META_DO_NOT_TRACE = "_Kafka_DO_NOT_TRACE";
    public static final String META_TRANSACTION_ID = "_Kafka_TRASACTION_ID";
    public static final String META_SPAN_ID = "_Kafka_SPAN_ID";
    public static final String META_PARENT_SPAN_ID = "_Kafka_PARENT_SPAN_ID";
    public static final String META_PARENT_APPLICATION_NAME = "_Kafka_PARENT_APPLICATION_NAME";
    public static final String META_PARENT_APPLICATION_TYPE = "_Kafka_PARENT_APPLICATION_TYPE";
    public static final String META_FLAGS = "_Kafka_FLAGS";

}
