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
package com.netflix.astyanax.cql.writes;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer;
import com.netflix.astyanax.serializers.ComparatorType;

public class CFMutationQueryGen {

	private static final Logger Logger = LoggerFactory.getLogger(CFMutationQueryGen.class);

    public enum MutationType {
		ColumnUpdate, ColumnDelete, RowDelete, counterColumnUpdate
	}

	// Constants that are used frequently for constructing the query
	private static final String INSERT_INTO  = "INSERT INTO ";
	private static final String OPEN_PARA  = " (";
	private static final String CLOSE_PARA  = ") ";
	private static final String VALUES  = ") VALUES (";
	private static final String BIND_MARKER  = "?,";
	private static final String LAST_BIND_MARKER  = "?";
	private static final String COMMA  = ",";
	private static final String USING = " USING ";
	private static final String TTL = " TTL ";
	private static final String AND = " AND ";
	private static final String TIMESTAMP = " TIMESTAMP ";

	private static final String DELETE_FROM  = "DELETE FROM ";
	private static final String WHERE  = " WHERE ";
	private static final String EQUALS  = " = ";
	private static final String UPDATE  = " UPDATE ";
	private static final String SET  = " SET ";

	private final String keyspace; 
	private final CqlColumnFamilyDefinitionImpl cfDef;
	private final Session session;

	public CFMutationQueryGen(Session session, String keyspaceName, CqlColumnFamilyDefinitionImpl cfDefinition) {

		this.keyspace = keyspaceName;
		this.cfDef = cfDefinition;
		this.session = session;
	}

	private static void appendWriteOptions(StringBuilder sb, Integer ttl, Long timestamp) {

		if (ttl != null || timestamp != null) {
			sb.append(USING);
		}

		if (ttl != null) {
			sb.append(TTL).append(ttl);
		}

		if (timestamp != null) {
			if (ttl != null) {
				sb.append(AND);
			}
			sb.append(TIMESTAMP).append(timestamp);
		}	
	}	
	
	abstract class MutationQueryCache<M> {

		private final AtomicReference<PreparedStatement> cachedStatement = new AtomicReference<>(null);

		public abstract Callable<String> getQueryGen(M mutation);

		public void addToBatch(BatchStatement batch, M mutation, boolean useCaching) {
			batch.add(getBoundStatement(mutation, useCaching));
		}
		
		public BoundStatement getBoundStatement(M mutation, boolean useCaching) {
			
			PreparedStatement pStatement = getPreparedStatement(mutation, useCaching);
			return bindValues(pStatement, mutation);
		}
		
		public abstract BoundStatement bindValues(PreparedStatement pStatement, M mutation);
		
		public PreparedStatement getPreparedStatement(M mutation, boolean useCaching) {
			
			PreparedStatement pStatement = null;
			
			if (useCaching) {
				pStatement = cachedStatement.get();
			}
			
			if (pStatement == null) {
				try {
					String query = getQueryGen(mutation).call();
					pStatement = session.prepare(query);
					
					if (Logger.isDebugEnabled()) {
						Logger.debug("Query: " + pStatement.getQueryString());
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			
			if (useCaching && cachedStatement.get() == null) {
				cachedStatement.set(pStatement);
			}
			return pStatement;
		}
	}


	private MutationQueryCache<CqlColumnListMutationImpl<?,?>> deleteRowQuery = new MutationQueryCache<CqlColumnListMutationImpl<?,?>>() {

		private final Callable<String> queryGen = () -> DELETE_FROM + keyspace + "." + cfDef.getName() +
                WHERE + cfDef.getPartitionKeyColumnDefinition().getName() + EQUALS + LAST_BIND_MARKER;

		@Override
		public Callable<String> getQueryGen(CqlColumnListMutationImpl<?, ?> mutation) {
			return queryGen;
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnListMutationImpl<?, ?> mutation) {
			return pStatement.bind(mutation.getRowKey());
		}
	};

	abstract class BaseClusteringKeyMutation extends MutationQueryCache<CqlColumnMutationImpl<?,?>> {

		public abstract boolean isDeleteQuery();

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnMutationImpl<?,?> colMutation) {

			int size = cfDef.getPartitionKeyColumnDefinitionList().size() + cfDef.getClusteringKeyColumnDefinitionList().size();
			if (!isDeleteQuery()) {
				size += cfDef.getRegularColumnDefinitionList().size();  
			} else {
				// we don't need to add the value component here. Just the partition key and the clustering key
			}

			Object[] arr = new Object[size];

			int index = 0;

			arr[index++] = colMutation.getRowKey();

			ColumnFamily<?,?> cf = colMutation.cfContext.getColumnFamily();
			boolean isCompositeColumn = cf.getColumnSerializer().getComparatorType() == ComparatorType.COMPOSITETYPE;

			if (isCompositeColumn) {
				AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) cf.getColumnSerializer();
				for (ComponentSerializer<?> component : compSerializer.getComponents()) {
					try {
						arr[index++] = component.getFieldValueDirectly(colMutation.columnName);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			} else {
				arr[index++] = colMutation.columnName;
			}

			if (!isDeleteQuery()) {
				arr[index++] = colMutation.columnValue;
			}

			return pStatement.bind(arr);
		}
	}


	private BaseClusteringKeyMutation insertColumnWithClusteringKey = new BaseClusteringKeyMutation() {

		@Override
		public Callable<String> getQueryGen(final CqlColumnMutationImpl<?, ?> mutation) {
			return new Callable<String>() {
				@Override
				public String call() throws Exception {
					return genQuery().toString();
				}

				private StringBuilder genQuery() {
					
					/**
					 * e.g 
					 *    insert into t (key, column1, value) values ('a', '2' , 'a2') using ttl 86400 and timestamp = 1234444;
					 */

					int columnCount = 0; 

					StringBuilder sb = new StringBuilder(INSERT_INTO);
					sb.append(keyspace + ".").append(cfDef.getName());
					sb.append(OPEN_PARA);

					Iterator<ColumnDefinition> iter = cfDef.getPartitionKeyColumnDefinitionList().iterator();
					while (iter.hasNext()) {
						sb.append(iter.next().getName());
						columnCount++;
						if (iter.hasNext()) {
							sb.append(COMMA);
						}
					}

					iter = cfDef.getClusteringKeyColumnDefinitionList().iterator();
					if (iter.hasNext()) {
						sb.append(COMMA);
						while (iter.hasNext()) {
							sb.append(iter.next().getName());
							columnCount++;
							if (iter.hasNext()) {
								sb.append(COMMA);
							}
						}
					}

					iter = cfDef.getRegularColumnDefinitionList().iterator();
					if (iter.hasNext()) {
						sb.append(COMMA);
						while (iter.hasNext()) {
							sb.append(iter.next().getName());
							columnCount++;
							if (iter.hasNext()) {
								sb.append(COMMA);
							}
						}
					}

					sb.append(VALUES);
					for (int i=0; i<columnCount; i++) {
						if (i < (columnCount-1)) {
							sb.append(BIND_MARKER);
						} else {
							sb.append(LAST_BIND_MARKER);
						}
					}
					sb.append(CLOSE_PARA);

					appendWriteOptions(sb, mutation.getTTL(), mutation.getTimestamp());
					
					return sb;
				}	
			};
		}

		@Override
		public boolean isDeleteQuery() {
			return false;
		}
	};

	private BaseClusteringKeyMutation deleteColumnWithClusteringKey = new BaseClusteringKeyMutation() {

		@Override
		public Callable<String> getQueryGen(final CqlColumnMutationImpl<?, ?> mutation) {
			
			return new Callable<String>() {
				@Override
				public String call() throws Exception {
					return genQuery().toString();
				}

				private StringBuilder genQuery() {

					StringBuilder sb = new StringBuilder(DELETE_FROM);
					sb.append(keyspace + ".").append(cfDef.getName());

					appendWriteOptions(sb, mutation.getTTL(), mutation.getTimestamp());

					Iterator<ColumnDefinition> iter = cfDef.getPartitionKeyColumnDefinitionList().iterator();

					sb.append(WHERE);
					while (iter.hasNext()) {
						sb.append(iter.next().getName()).append(EQUALS).append(LAST_BIND_MARKER);
						if (iter.hasNext()) {
							sb.append(AND);
						}
					}

					iter = cfDef.getClusteringKeyColumnDefinitionList().iterator();
					if (iter.hasNext()) {
						sb.append(AND);
						while (iter.hasNext()) {
							sb.append(iter.next().getName()).append(EQUALS).append(LAST_BIND_MARKER);
							if (iter.hasNext()) {
								sb.append(AND);
							}
						}
					}


					return sb;
				}
			};
		}

		@Override
		public boolean isDeleteQuery() {
			return true;
		}
	};
	
	private MutationQueryCache<CqlColumnMutationImpl<?,?>> counterColumnUpdate = new MutationQueryCache<CqlColumnMutationImpl<?,?>>() {

		@Override
		public Callable<String> getQueryGen(final CqlColumnMutationImpl<?, ?> mutation) {
			return () -> {

                String valueAlias = cfDef.getRegularColumnDefinitionList().get(0).getName();


                StringBuilder sb = new StringBuilder();
                
                        sb.append(UPDATE).append(keyspace).append(".").append(cfDef.getName());
                appendWriteOptions(sb, mutation.getTTL(), mutation.getTimestamp());
                
                        sb.append(SET).append(valueAlias).append(" = ").append(valueAlias).append(" + ? ");

                Iterator<ColumnDefinition> iter = cfDef.getPartitionKeyColumnDefinitionList().iterator();

                sb.append(WHERE);
                while (iter.hasNext()) {
                    sb.append(iter.next().getName()).append(EQUALS).append(LAST_BIND_MARKER);
                    if (iter.hasNext()) {
                        sb.append(AND);
                    }
                }

                iter = cfDef.getClusteringKeyColumnDefinitionList().iterator();
                if (iter.hasNext()) {
                    sb.append(AND);
                    while (iter.hasNext()) {
                        sb.append(iter.next().getName()).append(EQUALS).append(LAST_BIND_MARKER);
                        if (iter.hasNext()) {
                            sb.append(AND);
                        }
                    }
                }

                return sb.toString();
            };
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnMutationImpl<?, ?> mutation) {
			
			int size = 1 + cfDef.getPartitionKeyColumnDefinitionList().size() + cfDef.getClusteringKeyColumnDefinitionList().size();

			Object[] arr = new Object[size];

			int index = 0;

			arr[index++] = mutation.columnValue;
			arr[index++] = mutation.getRowKey();

			ColumnFamily<?,?> cf = mutation.cfContext.getColumnFamily();
			boolean isCompositeColumn = cf.getColumnSerializer().getComparatorType() == ComparatorType.COMPOSITETYPE;

			if (isCompositeColumn) {
				AnnotatedCompositeSerializer<?> compSerializer = (AnnotatedCompositeSerializer<?>) cf.getColumnSerializer();
				for (ComponentSerializer<?> component : compSerializer.getComponents()) {
					try {
						arr[index++] = component.getFieldValueDirectly(mutation.columnName);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			} else {
				arr[index++] = mutation.columnName;
			}

			return pStatement.bind(arr);
		}
	};

	private MutationQueryCache<CqlColumnListMutationImpl<?,?>> insertOrDeleteWithClusteringKey = new MutationQueryCache<CqlColumnListMutationImpl<?,?>>() {

		@Override
		public void addToBatch(BatchStatement batch, CqlColumnListMutationImpl<?,?> colListMutation, boolean useCaching) {
			
			for (CqlColumnMutationImpl<?,?> colMutation : colListMutation.getMutationList()) {

				switch (colMutation.getType()) {

				case UpdateColumn :
					insertColumnWithClusteringKey.addToBatch(batch, colMutation, useCaching);
					break;
				case DeleteColumn : 
					deleteColumnWithClusteringKey.addToBatch(batch, colMutation, useCaching);
					break;
				case CounterColumn : 
					throw new RuntimeException("Counter column update not allowed with other updates");
				default:
					throw new RuntimeException("Unsupported type: " + colMutation.getType());
				}
			}
		}

		@Override
		public Callable<String> getQueryGen(CqlColumnListMutationImpl<?, ?> colListMutation) {
			throw new RuntimeException("Not Supported");
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnListMutationImpl<?, ?> colListMutation) {
			throw new RuntimeException("Not Supported");
		}
	};

	private MutationQueryCache<CqlColumnListMutationImpl<?,?>> insertOrDeleteColumnListWithClusteringKey = new MutationQueryCache<CqlColumnListMutationImpl<?,?>>() {

		@Override
		public void addToBatch(BatchStatement batch, CqlColumnListMutationImpl<?,?> colListMutation, boolean useCaching) {
			
			for (CqlColumnMutationImpl<?,?> colMutation : colListMutation.getMutationList()) {
				insertOrDeleteColumnWithClusteringKey.addToBatch(batch, colMutation, useCaching);
			}
		}

		@Override
		public Callable<String> getQueryGen(CqlColumnListMutationImpl<?, ?> colListMutation) {
			throw new RuntimeException("Not Supported");
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnListMutationImpl<?, ?> colListMutation) {
			throw new RuntimeException("Not Supported");
		}
	};

	private MutationQueryCache<CqlColumnMutationImpl<?,?>> insertOrDeleteColumnWithClusteringKey = new MutationQueryCache<CqlColumnMutationImpl<?,?>>() {

		@Override
		public BoundStatement getBoundStatement(CqlColumnMutationImpl<?, ?> mutation, boolean useCaching) {
			switch (mutation.getType()) {

			case UpdateColumn :
				return insertColumnWithClusteringKey.getBoundStatement(mutation, useCaching);
			case DeleteColumn : 
				return deleteColumnWithClusteringKey.getBoundStatement(mutation, useCaching);
			case CounterColumn : 
				return counterColumnUpdate.getBoundStatement(mutation, useCaching);
			default:
				throw new RuntimeException("Unsupported type: " + mutation.getType());
			}
		}

		@Override
		public Callable<String> getQueryGen(CqlColumnMutationImpl<?, ?> colMutation) {
			throw new RuntimeException("Not Supported");
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnMutationImpl<?, ?> colMutation) {
			throw new RuntimeException("Not Supported");
		}
	};


	private MutationQueryCache<CqlColumnListMutationImpl<?,?>> counterColumnList = new MutationQueryCache<CqlColumnListMutationImpl<?,?>>() {

		@Override
		public void addToBatch(BatchStatement batch, CqlColumnListMutationImpl<?,?> colListMutation, boolean useCaching) {
			
			for (CqlColumnMutationImpl<?,?> colMutation : colListMutation.getMutationList()) {
				counterColumnUpdate.addToBatch(batch, colMutation, useCaching);
			}
		}

		@Override
		public Callable<String> getQueryGen(CqlColumnListMutationImpl<?, ?> colListMutation) {
			throw new RuntimeException("Not Supported");
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnListMutationImpl<?, ?> colListMutation) {
			throw new RuntimeException("Not Supported");
		}
	};
	
	private MutationQueryCache<CqlColumnListMutationImpl<?,?>> flatTableInsertQuery = new MutationQueryCache<CqlColumnListMutationImpl<?,?>> () {

		@Override
		public void addToBatch(BatchStatement batch, CqlColumnListMutationImpl<?, ?> colListMutation, boolean useCaching) {

			StringBuilder sb = new StringBuilder();
			sb.append(INSERT_INTO).append(keyspace + ".").append(cfDef.getName());
			sb.append(OPEN_PARA);

			// Init the object array for the bind values
			int size = colListMutation.getMutationList().size() + 1;
			Object[] values = new Object[size];
			int index = 0;
			
			// Add in the primary key
			sb.append(cfDef.getPartitionKeyColumnDefinition().getName()).append(COMMA);
			values[index++] = colListMutation.getRowKey();
			
			for (CqlColumnMutationImpl<?,?> colMutation : colListMutation.getMutationList()) {
				sb.append(colMutation.columnName);
				values[index++] = colMutation.columnValue;
				if (index < size) {
					sb.append(COMMA);
				}
			}
			
			sb.append(VALUES); 
			
			for (int i=0; i<size; i++) {
				if (i < (size-1)) {
					sb.append(BIND_MARKER);
				} else {
					sb.append(LAST_BIND_MARKER);
				}
			}
			
			sb.append(CLOSE_PARA);
			
			appendWriteOptions(sb, colListMutation.getDefaultTtl(), colListMutation.getTimestamp());
			
			String query = sb.toString(); 
			
			if (Logger.isDebugEnabled()) {
				Logger.debug("Query: " + query);
			}
			
			try {
				PreparedStatement pStatement = session.prepare(query);
				batch.add(pStatement.bind(values));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Callable<String> getQueryGen(CqlColumnListMutationImpl<?, ?> mutation) {
			throw new RuntimeException("Not Supported");
		}

		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnListMutationImpl<?, ?> mutation) {
			throw new RuntimeException("Not Supported");
		}
	};

	private MutationQueryCache<CqlColumnMutationImpl<?,?>> flatTableInsertQueryForColumn = new MutationQueryCache<CqlColumnMutationImpl<?,?>> () {

		@Override
		public Callable<String> getQueryGen(CqlColumnMutationImpl<?, ?> mutation) {
			throw new RuntimeException("Not Supported");
		}


		@Override
		public BoundStatement bindValues(PreparedStatement pStatement, CqlColumnMutationImpl<?, ?> mutation) {
			throw new RuntimeException("Not Supported");
		}
		
		@Override
		public void addToBatch(BatchStatement batch, CqlColumnMutationImpl<?, ?> mutation, boolean useCaching) {
			throw new RuntimeException("Not Supported");
		}
		
		@Override
		public BoundStatement getBoundStatement(CqlColumnMutationImpl<?, ?> mutation, boolean useCaching) {

			StringBuilder sb = new StringBuilder();
			sb.append(INSERT_INTO).append(keyspace + ".").append(cfDef.getName());
			sb.append(OPEN_PARA);

			sb.append(cfDef.getPartitionKeyColumnDefinition().getName());
			sb.append(COMMA);
			sb.append(mutation.columnName);

			sb.append(VALUES); 
			sb.append(BIND_MARKER);
			sb.append(LAST_BIND_MARKER);
			sb.append(CLOSE_PARA);
			
			appendWriteOptions(sb, mutation.getTTL(), mutation.getTimestamp());
			
			String query = sb.toString(); 
			
			if (Logger.isDebugEnabled()) {
				Logger.debug("Query: " + query);
			}

			// Init the object array for the bind values
			Object[] values = new Object[2];
			values[0] = mutation.getRowKey();
			values[1] = mutation.columnValue;
			
			try {
				PreparedStatement pStatement = session.prepare(query);
				return pStatement.bind(values);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	};


	public void addColumnListMutationToBatch(BatchStatement batch, CqlColumnListMutationImpl<?,?> colListMutation, boolean useCaching) {

		switch (colListMutation.getType()) {
		case RowDelete:
			deleteRowQuery.addToBatch(batch, colListMutation, useCaching);
			break;
		case ColumnsUpdate:
			if (cfDef.getClusteringKeyColumnDefinitionList().isEmpty()) {
				// THIS IS A FLAT TABLE QUERY
				flatTableInsertQuery.addToBatch(batch, colListMutation, useCaching);
			} else {
				insertOrDeleteWithClusteringKey.addToBatch(batch, colListMutation, useCaching);
			}
			break;
		case CounterColumnsUpdate:
			counterColumnList.addToBatch(batch, colListMutation, useCaching);
			break;
		default:
			throw new RuntimeException("Unrecognized ColumnListMutation Type");
		}
	}

	public BoundStatement getColumnMutationStatement(CqlColumnMutationImpl<?,?> mutation, boolean useCaching) {

		if (cfDef.getClusteringKeyColumnDefinitionList().isEmpty()) {
			// THIS IS A FLAT TABLE QUERY
			return flatTableInsertQueryForColumn.getBoundStatement(mutation, useCaching);
		} else {
			return insertOrDeleteColumnWithClusteringKey.getBoundStatement(mutation, useCaching);
		}
	}
}
