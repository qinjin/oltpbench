package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.util.Random;

import mdtc.api.transaction.client.ResultSet;
import mdtc.api.transaction.client.Row;
import mdtc.api.transaction.client.TransactionClient;

import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class DeliveryExt extends MDTCProcedure {
    
    private static final String DELIVERY_UPDATE_CUST_BAL_DELIVERY_COUNT = "DELIVERY_UPDATE_CUST_BAL_DELIVERY_COUNT";
    private static final String DELIVERY_SUM_ORDER_AMOUNT = "DELIVERY_SUM_ORDER_AMOUNT";
    private static final String DELIVERY_UPDATE_DELIVERY_DATE = "DELIVERY_UPDATE_DELIVERY_DATE";
    private static final String DELIVERY_UPDATE_CARRIER_ID = "DELIVERY_UPDATE_CARRIER_ID";
    private static final String DELIVERY_GET_CUST_ID = "DELIVERY_GET_CUST_ID";
    private static final String DELIVERY_DELETE_NEW_ORDER = "DELIVERY_DELETE_NEW_ORDER";
    private static final String DELIVERY_GET_ORDER_ID = "DELIVERY_GET_ORDER_ID";
    
    public static final String STMT_GET_ORDER_ID = "SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE NO_D_ID = ?" + " AND NO_W_ID = ? ORDER BY NO_O_ID ASC LIMIT 1";
    public static final String STMT_DELETE_NEW_ORDER = "SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE NO_D_ID = ?" + " AND NO_W_ID = ? ORDER BY NO_O_ID ASC LIMIT 1";
    public static final String STMT_GET_CUST_ID = "SELECT O_C_ID" + " FROM " + TPCCConstants.TABLENAME_OPENORDER + " WHERE O_ID = ?" + " AND O_D_ID = ?" + " AND O_W_ID = ?";
    public static final String STMT_UPDATE_CARRIER_ID = "UPDATE " + TPCCConstants.TABLENAME_OPENORDER + " SET O_CARRIER_ID = ?" + " WHERE O_ID = ?" + " AND O_D_ID = ?" + " AND O_W_ID = ?";
    public static final String STMT_UPDATE_DELIVERY_DATE = "UPDATE " + TPCCConstants.TABLENAME_ORDERLINE + " SET OL_DELIVERY_D = ?" + " WHERE OL_O_ID = ?" + " AND OL_D_ID = ?" + " AND OL_W_ID = ?";
    public static final String STMT_SUM_ORDER_AMOUNT = "SELECT SUM(OL_AMOUNT) AS OL_TOTAL" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + "" + " WHERE OL_O_ID = ?" + " AND OL_D_ID = ?"
            + " AND OL_W_ID = ?";
    public static final String STMT_UPDATE_CUST_BAL_DELIVERY_COUNT = "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = C_BALANCE + ?" + ", C_DELIVERY_CNT = C_DELIVERY_CNT + 1"
            + " WHERE C_W_ID = ?" + " AND C_D_ID = ?" + " AND C_ID = ?";

    public void run(TransactionClient txnClient, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        initStatements(txnClient);
        int orderCarrierID = TPCCUtil.randomNumber(1, 10, gen);
        deliveryTransaction(terminalWarehouseID, orderCarrierID, txnClient, w);
    }

    public void initStatements(TransactionClient txnClient) {
        txnClient.setPrepareStatement(DELIVERY_GET_ORDER_ID, STMT_GET_ORDER_ID);
        txnClient.setPrepareStatement(DELIVERY_DELETE_NEW_ORDER, STMT_DELETE_NEW_ORDER);
        txnClient.setPrepareStatement(DELIVERY_GET_CUST_ID, STMT_GET_CUST_ID);
        txnClient.setPrepareStatement(DELIVERY_UPDATE_CARRIER_ID, STMT_UPDATE_CARRIER_ID);
        txnClient.setPrepareStatement(DELIVERY_UPDATE_DELIVERY_DATE, STMT_UPDATE_DELIVERY_DATE);
        txnClient.setPrepareStatement(DELIVERY_SUM_ORDER_AMOUNT, STMT_SUM_ORDER_AMOUNT);
        txnClient.setPrepareStatement(DELIVERY_UPDATE_CUST_BAL_DELIVERY_COUNT, STMT_UPDATE_CUST_BAL_DELIVERY_COUNT);
    }

    private int deliveryTransaction(int w_id, int o_carrier_id, TransactionClient txnClient, TPCCWorker w) {

        int d_id, c_id;
        float ol_total;
        int[] orderIDs;

        orderIDs = new int[10];
        ResultSet rs;
        Row resultRow;

        for (d_id = 1; d_id <= 10; d_id++) {
            // statement = new BoundStatement(delivGetOrderId).bind(1,
            // d_id).bind(2, w_id);
            rs = txnClient.executePreparedStatement(DELIVERY_GET_ORDER_ID, d_id, w_id);
            if (!rs.iterator().hasNext()) {
                // This district has no new orders; this can happen but should
                // be rare
                continue;
            }

            resultRow = rs.iterator().next();
            int no_o_id = resultRow.getInt("NO_O_ID");
            orderIDs[d_id - 1] = no_o_id;
            rs = null;

            // statement = new BoundStatement(delivDeleteNewOrder).bind(1,
            // no_o_id).bind(2, d_id).bind(3, w_id);
            rs = txnClient.executePreparedStatement(DELIVERY_DELETE_NEW_ORDER, no_o_id, d_id, w_id);
            if (!rs.isEmpty()) {
                // This code used to run in a loop in an attempt to make this
                // work
                // with MySQL's default weird consistency level. We just always
                // run
                // this as SERIALIZABLE instead. I don't *think* that fixing
                // this one
                // error makes this work with MySQL's default consistency.
                // Careful
                // auditing would be required.
                throw new UserAbortException("New order w_id=" + w_id + " d_id=" + d_id + " no_o_id=" + no_o_id + " delete failed (not running with SERIALIZABLE isolation?)");
            }

            // statement = new BoundStatement(delivGetCustId).bind(1,
            // no_o_id).bind(2, d_id).bind(3, w_id);
            rs = txnClient.executePreparedStatement(DELIVERY_GET_CUST_ID, no_o_id, d_id, w_id);
            if (rs.isEmpty())
                throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");
            resultRow = rs.iterator().next();
            c_id = resultRow.getInt("O_C_ID");
            rs = null;

            // statement = new BoundStatement(delivUpdateCarrierId).bind(1,
            // o_carrier_id).bind(2, no_o_id).bind(3, d_id).bind(4, w_id);
            rs = txnClient.executePreparedStatement(DELIVERY_UPDATE_CARRIER_ID, o_carrier_id, no_o_id, d_id, w_id);
            if (rs.isEmpty())
                throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");

            // statement = new BoundStatement(delivUpdateDeliveryDate).bind(1,
            // System.currentTimeMillis()).bind(2, no_o_id).bind(3,
            // d_id).bind(4, w_id);
            rs = txnClient.executePreparedStatement(DELIVERY_UPDATE_DELIVERY_DATE, System.currentTimeMillis(), no_o_id, d_id, w_id);
            if (rs.isEmpty())
                throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");

            // statement = new BoundStatement(delivSumOrderAmount).bind(1,
            // no_o_id).bind(2, d_id).bind(3, w_id);
            rs = txnClient.executePreparedStatement(DELIVERY_SUM_ORDER_AMOUNT, no_o_id, d_id, w_id);
            if (rs.isEmpty())
                throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");
            resultRow = rs.iterator().next();
            ol_total = resultRow.getFloat("OL_TOTAL");
            rs = null;

            // statement = new
            // BoundStatement(delivUpdateCustBalDelivCnt).bind(1,
            // ol_total).bind(2, w_id).bind(3, d_id).bind(4, c_id);
            rs = txnClient.executePreparedStatement(DELIVERY_UPDATE_CUST_BAL_DELIVERY_COUNT, ol_total, w_id, d_id, c_id);
            if (rs.isEmpty())
                throw new RuntimeException("C_ID=" + c_id + " C_W_ID=" + w_id + " C_D_ID=" + d_id + " not found!");
        }

        // TODO: This part is not used
        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage.append("\n+---------------------------- DELIVERY ---------------------------+\n");
        terminalMessage.append(" Date: ");
        terminalMessage.append(TPCCUtil.getCurrentTime());
        terminalMessage.append("\n\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n Carrier:   ");
        terminalMessage.append(o_carrier_id);
        terminalMessage.append("\n\n Delivered Orders\n");
        int skippedDeliveries = 0;
        for (int i = 1; i <= 10; i++) {
            if (orderIDs[i - 1] >= 0) {
                terminalMessage.append("  District ");
                terminalMessage.append(i < 10 ? " " : "");
                terminalMessage.append(i);
                terminalMessage.append(": Order number ");
                terminalMessage.append(orderIDs[i - 1]);
                terminalMessage.append(" was delivered.\n");
            } else {
                terminalMessage.append("  District ");
                terminalMessage.append(i < 10 ? " " : "");
                terminalMessage.append(i);
                terminalMessage.append(": No orders to be delivered.\n");
                skippedDeliveries++;
            }
        }
        terminalMessage.append("+-----------------------------------------------------------------+\n\n");

        return skippedDeliveries;
    }
}
