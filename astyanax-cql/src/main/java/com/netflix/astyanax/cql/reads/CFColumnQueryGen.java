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
package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Builder;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer;

public class CFColumnQueryGen {

    private final AtomicReference<Session> sessionRef = new AtomicReference<>(null);
	private final String keyspace; 
	private final CqlColumnFamilyDefinitionImpl cfDef;

	private final String partitionKeyCol;
	private final List<ColumnDefinition> clusteringKeyCols;
	private final List<ColumnDefinition> regularCols;

    private final boolean isCompositeColumn;
    private final boolean isFlatTable;
	
	private static final String BIND_MARKER = "?";

	

	public CFColumnQueryGen(Session session, String keyspaceName, CqlColumnFamilyDefinitionImpl cfDefinition) {

		this.keyspace = keyspaceName;
		this.cfDef = cfDefinition;
		this.sessionRef.set(session);
		
		partitionKeyCol = cfDef.getPartitionKeyColumnDefinition().getName();
		clusteringKeyCols = cfDef.getClusteringKeyColumnDefinitionList();
		regularCols = cfDef.getRegularColumnDefinitionList();
		
		isCompositeColumn = clusteringKeyCols.size() > 1;
		isFlatTable = clusteringKeyCols.isEmpty();
	}

    private final QueryGenCache<CqlColumnQueryImpl<?>> columnQueryWithClusteringKey = new QueryGenCache<CqlColumnQueryImpl<?>>(sessionRef) {

        @Override
        public Callable<RegularStatement> getQueryGen(final CqlColumnQueryImpl<?> columnQuery) {

            return () -> {
                if (clusteringKeyCols.size() != 1) {
                    throw new RuntimeException("Cannot use this query for this schema, clustetingKeyCols.size: " + clusteringKeyCols.size());
                }

                String valueColName = regularCols.get(0).getName();

                return QueryBuilder.select()
                        .column(valueColName).ttl(valueColName).writeTime(valueColName)
                        .from(keyspace, cfDef.getName())
                        .where(eq(partitionKeyCol, BIND_MARKER))
                        .and(eq(clusteringKeyCols.get(0).getName(), BIND_MARKER));
            };
        }

        @Override
        public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnQueryImpl<?> columnQuery) {
            return pStatement.bind(columnQuery.getRowKey(), columnQuery.getColumnName());
        }
    };

    private final QueryGenCache<CqlColumnQueryImpl<?>> columnQueryWithCompositeColumn = new QueryGenCache<CqlColumnQueryImpl<?>>(sessionRef) {

        @Override
        public Callable<RegularStatement> getQueryGen(final CqlColumnQueryImpl<?> columnQuery) {

            return () -> {

                if (clusteringKeyCols.size() <= 1) {
                    throw new RuntimeException("Cannot use this query for this schema, clustetingKeyCols.size: " + clusteringKeyCols.size());
                }

                String valueColName = regularCols.get(0).getName();

                ColumnFamily<?, ?> cf = columnQuery.getCF();
                AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) cf.getColumnSerializer();
                List<ComponentSerializer<?>> components = compSerializer.getComponents();

                // select the individual columns as dictated by the no of component serializers
                Builder select = QueryBuilder.select()
                        .column(valueColName).ttl(valueColName).writeTime(valueColName);

                Where where = select.from(keyspace, cfDef.getName()).where(eq(partitionKeyCol, BIND_MARKER));

                for (int index = 0; index < components.size(); index++) {
                    where.and(eq(clusteringKeyCols.get(index).getName(), BIND_MARKER));
                }

                return where;
            };
        }

        @Override
        public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnQueryImpl<?> columnQuery) {

            List<Object> values = new ArrayList<>();
            values.add(columnQuery.getRowKey());

            ColumnFamily<?, ?> cf = columnQuery.getCF();
            AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) cf.getColumnSerializer();
            List<ComponentSerializer<?>> components = compSerializer.getComponents();

            Object columnName = columnQuery.getColumnName();
            for (ComponentSerializer<?> component : components) {
                values.add(component.getFieldValueDirectly(columnName));
            }

            return pStatement.bind(values.toArray());
        }
    };


    private final QueryGenCache<CqlColumnQueryImpl<?>> flatTableColumnQuery = new QueryGenCache<CqlColumnQueryImpl<?>>(sessionRef) {

        @Override
        public Callable<RegularStatement> getQueryGen(final CqlColumnQueryImpl<?> columnQuery) {

            return () -> {

                if (!clusteringKeyCols.isEmpty()) {
                    throw new RuntimeException("Cannot use this query for this schema, clustetingKeyCols.size: " + clusteringKeyCols.size());
                }

                String columnNameString = (String) columnQuery.getColumnName();
                return QueryBuilder.select()
                        .column(columnNameString).ttl(columnNameString).writeTime(columnNameString)
                        .from(keyspace, cfDef.getName())
                        .where(eq(partitionKeyCol, BIND_MARKER));
            };
        }

        @Override
        public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnQueryImpl<?> columnQuery) {
            return pStatement.bind(columnQuery.getRowKey());
        }
    };
	
	public BoundStatement getQueryStatement(CqlColumnQueryImpl<?> columnQuery, boolean useCaching) {

		if (isFlatTable) {
			return flatTableColumnQuery.getBoundStatement(columnQuery, useCaching);
		}
		
		if (isCompositeColumn) {
			return columnQueryWithCompositeColumn.getBoundStatement(columnQuery, useCaching);
		} else {
			return columnQueryWithClusteringKey.getBoundStatement(columnQuery, useCaching);
		}
	}
}
