package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import mdtc.api.transaction.client.TransactionClient;

import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.procedures.TPCCProcedure;

public abstract class MDTCProcedure extends TPCCProcedure{
    protected int numCQLRead, numCQLWrite;
    
    public ResultSet run(Connection conn, Random gen,
            int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID,
            TPCCWorker w) throws SQLException{
        throw new RuntimeException("Not supported");
    }
    
    public abstract void run(TransactionClient txnClient, Random gen,
            int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID,
            TPCCWorker w);
    
    public abstract void initStatements(TransactionClient txnClient);
    

    public int numCQLReadRequests() {
        return numCQLRead;
    }

    public int numCQLWriteRequests() {
        return numCQLWrite;
    }
}
