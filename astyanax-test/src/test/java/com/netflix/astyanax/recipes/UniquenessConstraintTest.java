package com.netflix.astyanax.recipes;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@Ignore
public class UniquenessConstraintTest {

    private static Logger log = LoggerFactory
            .getLogger(UniquenessConstraintTest.class);

    private static AstyanaxContext<Cluster> clusterContext;

    private static final String TEST_CLUSTER_NAME = "TestCluster";
    private static final String TEST_KEYSPACE_NAME = "UniqueIndexTest";
    private static final String TEST_DATA_CF = "UniqueRowKeyTest";

    private static final boolean TEST_INIT_KEYSPACE = true;

    private static ColumnFamily<Long, String> cfData = ColumnFamily
            .newColumnFamily(TEST_DATA_CF, LongSerializer.get(),
                    StringSerializer.get());

    @BeforeClass
    public static void setup() throws Exception {
        clusterContext = new AstyanaxContext.Builder()
                .forCluster(TEST_CLUSTER_NAME)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME)
                                .setMaxConnsPerHost(1).setSeeds(
                                        "localhost:7102"))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildCluster(ThriftFamilyFactory.getInstance());

        clusterContext.start();

        if (TEST_INIT_KEYSPACE) {
            Cluster cluster = clusterContext.getEntity();
            try {
                log.info("Dropping keyspace: " + TEST_KEYSPACE_NAME);
                cluster.dropKeyspace(TEST_KEYSPACE_NAME);
                Thread.sleep(10000);
            } catch (ConnectionException e) {
                log.warn(e.getMessage());
            }

            Map<String, String> stratOptions = new HashMap<>();
            stratOptions.put("replication_factor", "3");

            try {
                log.info("Creating keyspace: " + TEST_KEYSPACE_NAME);

                KeyspaceDefinition ksDef = cluster.makeKeyspaceDefinition();

                ksDef.setName(TEST_KEYSPACE_NAME)
                        .setStrategyOptions(stratOptions)
                        .setStrategyClass("SimpleStrategy")
                        .addColumnFamily(
                                cluster.makeColumnFamilyDefinition()
                                        .setName(cfData.getName())
                                        .setComparatorType("UTF8Type"));
                cluster.addKeyspace(ksDef);
                Thread.sleep(1000);
            } catch (ConnectionException e) {
                log.error(e.getMessage());
            }
        }
    }

    @AfterClass
    public static void teardown() {
        if (clusterContext != null) {
            clusterContext.shutdown();
        }
    }

    @Test
    public void testUniqueness() throws Exception {
        log.info("Starting");

        Keyspace keyspace = clusterContext.getEntity().getKeyspace(TEST_KEYSPACE_NAME);

        UniquenessConstraintWithPrefix<Long> unique = new UniquenessConstraintWithPrefix<Long>(
                keyspace, cfData)
                .setTtl(2)
                .setPrefix("unique_")
                .setConsistencyLevel(ConsistencyLevel.CL_ONE)
                .setMonitor(
                (key, column) -> {
                    log.info("Violated: " + key + " column: "
                            + column);
                });

        try {
            String column = unique.isUnique(1234L);
            Assert.assertNotNull(column);
            log.info(column);
            column = unique.isUnique(1234L);
            Assert.assertNull(column);

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
            }
            column = unique.isUnique(1234L);
            Assert.assertNotNull(column);
            log.info(column);
        } catch (ConnectionException e) {
            log.error(e.getMessage());
            Assert.fail(e.getMessage());
        }
    }

}
