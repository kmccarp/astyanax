/**
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.astyanax.cql.test;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.test.MockCompositeTypeTests.MockCompositeType;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class CompositeKeyTests extends KeyspaceTests {

	private static final Logger LOG = Logger.getLogger(CompositeKeyTests.class);
	
	@BeforeClass
	public static void init() throws Exception {
		initContext();
		
		keyspace.createColumnFamily(cfCompositeKey, ImmutableMap.<String, Object>builder()
				.put("key_validation_class", "BytesType")
				.build());
		 
        cfCompositeKey.describe(keyspace);
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		keyspace.dropColumnFamily(cfCompositeKey);
	}

	private static AnnotatedCompositeSerializer<MockCompositeType> mSerializer 
    	= new AnnotatedCompositeSerializer<>(MockCompositeType.class);

    private static ColumnFamily<MockCompositeType, String> cfCompositeKey 
    	= ColumnFamily.newColumnFamily("compositekey", mSerializer, StringSerializer.get());
	
    @Test
    public void testCompositeKey() {
        MockCompositeType key = new MockCompositeType("A", 1, 2, true, "B");
        MutationBatch m = keyspace.prepareMutationBatch();
        m.withRow(cfCompositeKey, key).putColumn("Test", "Value", null);
        try {
            m.execute();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        try {
            ColumnList<String> row = keyspace.prepareQuery(cfCompositeKey)
                    .getKey(key).execute().getResult();
            Assert.assertFalse(row.isEmpty());
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }
    }
}
