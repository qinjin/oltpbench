package com.oltpbenchmark.util;

import mdtc.impl.APIFactory;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class MDTCResult {
    private static final Logger logger = Logger.getLogger(MDTCResult.class);

    public int dcNo;
    public int viewLength;
    public int executionDelay;
    public int txnType;
    public int numClients;
    public String evaType;
    public int succeedTxns;
    public int abortedTxns;
    public int numCQLRead;
    public int numCQLWrite;
    public long benchmarkTime;
    public double txnThroughput;
    public double cqlThroughput;

    private static final String INSERT_CQL = "INSERT INTO RESULT" + " (dc_no,view_length, exe_delay, type, num_clients, eva_type, succeed_txns,"
            + " aborted_txns, num_cql_read, num_cql_write, benchmark_time, throughput, cql_throughput)" + " values (";

    public void saveToCassandra() {
        logger.info("Saving result to Cassandra...");
        Cluster cluster = Cluster.builder().addContactPoint(APIFactory.getResultServer()).build();
        Session session = cluster.connect("mdtc_tpcc");

        String cql = INSERT_CQL + dcNo + "," + viewLength + "," + executionDelay + "," + txnType + "," + numClients + ", '" + evaType + "' ," + succeedTxns + "," + abortedTxns + "," + numCQLRead + ","
                + numCQLWrite + "," + benchmarkTime + "," + txnThroughput + "," + cqlThroughput + ")";
        session.execute(cql);

        logger.info("Done Save result");
        cluster.close();
    }
}
