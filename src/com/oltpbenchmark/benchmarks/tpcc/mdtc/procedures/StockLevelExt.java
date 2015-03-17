package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.util.Random;

import org.apache.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures.MDTCProcedure.Result;
import com.oltpbenchmark.benchmarks.tpcc.procedures.TPCCProcedure;

public class StockLevelExt extends MDTCProcedure {

    private static final Logger LOG = Logger.getLogger(StockLevelExt.class);

    private final String STMT_GET_DIST_ORDER = "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?";
    private final String STMT_GET_COUNT_STOCK = "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK
            + " WHERE OL_W_ID = ?" + " AND OL_D_ID = ?" + " AND OL_O_ID < ?" + " AND OL_O_ID >= ? - 20" + " AND S_W_ID = ?" + " AND S_I_ID = OL_I_ID" + " AND S_QUANTITY < ?";

    // Stock Level Txn
    private PreparedStatement stockGetDistOrderId = null;
    private PreparedStatement stockGetCountStock = null;

    public Result run(Session session, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        // initializing all prepared statements
        initStatements(session);

        int threshold = TPCCUtil.randomNumber(10, 20, gen);

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);

        stockLevelTransaction(terminalWarehouseID, districtID, threshold, session, w);

        return null;
    }

    public void initStatements(Session session) {
        stockGetDistOrderId = session.prepare(STMT_GET_DIST_ORDER);
        stockGetCountStock = session.prepare(STMT_GET_COUNT_STOCK);
    }

    private void stockLevelTransaction(int w_id, int d_id, int threshold, Session session, TPCCWorker w) {
        int o_id = 0;
        // XXX int i_id = 0;
        int stock_count = 0;

        // XXX District dist = new District();
        // XXX OrderLine orln = new OrderLine();
        // XXX Stock stck = new Stock();

        BoundStatement statement;
        ResultSet rs;
        Row resultRow;
        
        statement = new BoundStatement(stockGetDistOrderId).bind(1, w_id).bind(2, d_id);
        rs = session.execute(statement);

        if (!rs.iterator().hasNext())
            throw new RuntimeException("D_W_ID=" + w_id + " D_ID=" + d_id + " not found!");
        resultRow = rs.iterator().next();
        o_id = resultRow.getInt("D_NEXT_O_ID");
        rs = null;

        statement = new BoundStatement(stockGetCountStock).bind(1, w_id).bind(2, d_id).bind(3, o_id).bind(4, o_id).bind(5, w_id).bind(6, threshold);
        rs = session.execute(statement);
        if (!rs.iterator().hasNext())
            throw new RuntimeException("OL_W_ID=" + w_id + " OL_D_ID=" + d_id + " OL_O_ID=" + o_id + " not found!");
        resultRow = rs.iterator().next();
        stock_count = resultRow.getInt("STOCK_COUNT");

        rs = null;

        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage.append("\n+-------------------------- STOCK-LEVEL --------------------------+");
        terminalMessage.append("\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n District:  ");
        terminalMessage.append(d_id);
        terminalMessage.append("\n\n Stock Level Threshold: ");
        terminalMessage.append(threshold);
        terminalMessage.append("\n Low Stock Count:       ");
        terminalMessage.append(stock_count);
        terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");
        if (LOG.isTraceEnabled())
            LOG.trace(terminalMessage.toString());
    }

}
