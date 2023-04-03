/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.test;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnType;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;

public class TestConstants {

    public static final ColumnFamily<String, String> CF_STANDARD1 = new ColumnFamily<>(
            "Standard1", StringSerializer.get(), StringSerializer.get());

    public static final ColumnFamily<String, Long> CF_LONGCOLUMN = new ColumnFamily<>(
            "LongColumn1", StringSerializer.get(), LongSerializer.get());

    public static final ColumnFamily<String, String> CF_STANDARD2 = new ColumnFamily<>(
            "Standard2", StringSerializer.get(), StringSerializer.get());

    public static final ColumnFamily<String, String> CF_SUPER1 = new ColumnFamily<>(
            "Super1", StringSerializer.get(), StringSerializer.get(),
            ColumnType.SUPER);

    public static final ColumnFamily<String, String> CF_COUNTER1 = new ColumnFamily<>(
            "Counter1", StringSerializer.get(), StringSerializer.get());

    public static final ColumnFamily<String, String> CF_COUNTER_SUPER1 = new ColumnFamily<>(
            "CounterSuper1", StringSerializer.get(), StringSerializer.get(),
            ColumnType.SUPER);

    public static final ColumnFamily<String, String> CF_NOT_DEFINED = new ColumnFamily<>(
            "NotDefined", StringSerializer.get(), StringSerializer.get());

    public static final ColumnFamily<String, String> CF_EMPTY = new ColumnFamily<>(
            "NotDefined", StringSerializer.get(), StringSerializer.get());

    public static final ColumnFamily<String, TestCompositeType> CF_COMPOSITE = new ColumnFamily<>(
            "CompositeColumn", StringSerializer.get(),
            new AnnotatedCompositeSerializer<TestCompositeType>(
                    TestCompositeType.class));

    public static final ColumnFamily<ByteBuffer, ByteBuffer> CF_COMPOSITE_CSV = new ColumnFamily<>(
            "CompositeCsv", ByteBufferSerializer.get(),
            ByteBufferSerializer.get());

    public static final ColumnFamily<TestCompositeType, String> CF_COMPOSITE_KEY = new ColumnFamily<>(
            "CompositeKey",
            new AnnotatedCompositeSerializer<TestCompositeType>(
                    TestCompositeType.class), StringSerializer.get());

    public static final ColumnFamily<String, UUID> CF_TIME_UUID = new ColumnFamily<>(
            "TimeUUID1", StringSerializer.get(), TimeUUIDSerializer.get());

    public static final AnnotatedCompositeSerializer<SessionEvent> SE_SERIALIZER = new AnnotatedCompositeSerializer<>(
            SessionEvent.class);

    public static final ColumnFamily<String, SessionEvent> CF_CLICK_STREAM = new ColumnFamily<>(
            "ClickStream", StringSerializer.get(), SE_SERIALIZER);

    public static final String CLUSTER_NAME = "TestCluster1";
    public static final String KEYSPACE_NAME = "Keyspace1";

}
