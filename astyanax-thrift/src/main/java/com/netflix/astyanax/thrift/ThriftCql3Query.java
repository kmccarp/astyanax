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
package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.netflix.astyanax.serializers.StringSerializer;

public class ThriftCql3Query<K,C> extends AbstractThriftCqlQuery<K, C> {

    ThriftCql3Query(ThriftColumnFamilyQueryImpl<K, C> cfQuery, String cql) {
        super(cfQuery, cql);
    }

    @Override
    protected org.apache.cassandra.thrift.CqlPreparedResult prepareCqlQuery(Client client) throws InvalidRequestException, TException {
        return client.prepare_cql3_query(StringSerializer.get().toByteBuffer(cql), Compression.NONE);
    }
    
    @Override
    protected org.apache.cassandra.thrift.CqlResult executePreparedCqlQuery(Client client, int id, List<ByteBuffer> values) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {
        return client.execute_prepared_cql3_query(id, values, ThriftConverter.toThriftConsistencyLevel(cl));
    }
    
    @Override
    protected org.apache.cassandra.thrift.CqlResult executeCqlQuery(Client client) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {
        return client.execute_cql3_query(
            StringSerializer.get().toByteBuffer(cql),
            useCompression ? Compression.GZIP : Compression.NONE,
                    ThriftConverter.toThriftConsistencyLevel(cl));
    }

}
