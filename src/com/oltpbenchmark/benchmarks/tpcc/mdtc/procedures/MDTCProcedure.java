package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import com.datastax.driver.core.Session;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.procedures.TPCCProcedure;

public abstract class MDTCProcedure extends TPCCProcedure{
    public ResultSet run(Connection conn, Random gen,
            int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID,
            TPCCWorker w) throws SQLException{
        throw new RuntimeException("Not supported");
    }
    
    public abstract Result run(Session session, Random gen,
            int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID,
            TPCCWorker w);
    
    //TODO:
    public static class Result{
        
    }
}
