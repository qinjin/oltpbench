package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.util.Random;

import mdtc.api.transaction.client.ResultSet;
import mdtc.api.transaction.client.Row;
import mdtc.api.transaction.client.TransactionClient;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class StockLevelExt extends MDTCProcedure {
    private static final Logger LOG = Logger.getLogger(StockLevelExt.class);

    private static final String SL_GET_COUNT_STOCK = "SL_GET_COUNT_STOCK";
    private static final String SL_GET_DIST_ORDER = "SL_GET_DIST_ORDER";

    private final String STMT_GET_DIST_ORDER = "SELECT D_NEXT_O_ID FROM " + TPCCConstants.TABLENAME_DISTRICT + " WHERE D_W_ID = ? AND D_ID = ?";
    private final String STMT_GET_COUNT_STOCK = "SELECT S_I_ID FROM " + TPCCConstants.TABLENAME_STOCK + " WHERE S_W_ID = ? AND S_QUANTITY = ?";

    public void run(TransactionClient txnClient, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        // initializing all prepared statements
        initStatements(txnClient);

        int threshold = TPCCUtil.randomNumber(10, 20, gen);

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);

        stockLevelTransaction(terminalWarehouseID, districtID, threshold, txnClient, w);
    }

    public void initStatements(TransactionClient txnClient) {
        txnClient.setPrepareStatement(SL_GET_DIST_ORDER, STMT_GET_DIST_ORDER);
        txnClient.setPrepareStatement(SL_GET_COUNT_STOCK, STMT_GET_COUNT_STOCK);
    }

    private void stockLevelTransaction(int w_id, int d_id, int threshold, TransactionClient txnClient, TPCCWorker w) {
        int o_id = 0;
        // XXX int i_id = 0;
        int stock_count = 0;

        // XXX District dist = new District();
        // XXX OrderLine orln = new OrderLine();
        // XXX Stock stck = new Stock();

        ResultSet rs;
        Row resultRow;

        // statement = new BoundStatement(stockGetDistOrderId).bind(1,
        // w_id).bind(2, d_id);
        rs = txnClient.executePreparedStatement(SL_GET_DIST_ORDER, w_id, d_id);

        if (!rs.iterator().hasNext())
            throw new RuntimeException("D_W_ID=" + w_id + " D_ID=" + d_id + " not found!");
        resultRow = rs.iterator().next();
        o_id = resultRow.getInt("D_NEXT_O_ID");
        rs = null;

        // statement = new BoundStatement(stockGetCountStock).bind(1,
        // w_id).bind(2, d_id).bind(3, o_id).bind(4, o_id).bind(5, w_id).bind(6,
        // threshold);
        rs = txnClient.executePreparedStatement(SL_GET_COUNT_STOCK, w_id, threshold);
        stock_count = rs.allRows().size();

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
