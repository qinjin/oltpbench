package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import mdtc.api.transaction.client.ResultSet;
import mdtc.api.transaction.client.Row;
import mdtc.api.transaction.client.TransactionClient;

import com.google.common.collect.Lists;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class DeliveryExt extends MDTCProcedure {

    public static final String DELIVERY_UPDATE_CUST_BAL_DELIVERY_COUNT = "DELIVERY_UPDATE_CUST_BAL_DELIVERY_COUNT";
    public static final String DELIVERY_SUM_ORDER_AMOUNT = "DELIVERY_SUM_ORDER_AMOUNT";
    public static final String DELIVERY_UPDATE_DELIVERY_DATE = "DELIVERY_UPDATE_DELIVERY_DATE";
    public static final String DELIVERY_UPDATE_CARRIER_ID = "DELIVERY_UPDATE_CARRIER_ID";
    public static final String DELIVERY_GET_CUST_ID = "DELIVERY_GET_CUST_ID";
    public static final String DELIVERY_DELETE_NEW_ORDER = "DELIVERY_DELETE_NEW_ORDER";
    public static final String DELIVERY_GET_ORDER_ID = "DELIVERY_GET_ORDER_ID";
    public static final String DELIVERY_GET_CUST_BAN = "DELIVERY_GET_CUST_BAN";
    public static final String DELIVERY_GET_DELIVERY_COUNT = "DELIVERY_GET_DELIVERY_COUNT";
    public static final String DELIVERY_GET_OL_NUMBER = "DELIVERY_GET_OL_NUMBER";

    public static final String STMT_GET_ORDER_ID = "SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE NO_W_ID = ? AND NO_D_ID = ?";
    public static final String STMT_DELETE_NEW_ORDER = "DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE NO_W_ID = ? AND NO_D_ID = ? AND NO_O_ID = ?";
    public static final String STMT_GET_CUST_ID = "SELECT O_C_ID" + " FROM " + TPCCConstants.TABLENAME_OPENORDER + " WHERE O_W_ID = ?" + " AND O_D_ID = ?" + " AND O_ID = ?";
    public static final String STMT_UPDATE_CARRIER_ID = "UPDATE " + TPCCConstants.TABLENAME_OPENORDER + " SET O_CARRIER_ID = ?" + " WHERE O_W_ID = ?" + " AND O_D_ID = ?" + " AND O_ID = ?";
    public static final String STMT_UPDATE_DELIVERY_DATE = "UPDATE " + TPCCConstants.TABLENAME_ORDERLINE + " SET OL_DELIVERY_D = ?" + " WHERE OL_W_ID = ?" + " AND OL_D_ID = ?"
            + " AND OL_O_ID = ? AND OL_NUMBER = ?";
    public static final String STMT_GET_OL_NUMBER = "SELECT OL_NUMBER FROM " + TPCCConstants.TABLENAME_ORDERLINE + " WHERE OL_W_ID = ?" + " AND OL_D_ID = ?" + " AND OL_O_ID = ?";
    public static final String STMT_SUM_ORDER_AMOUNT = "SELECT SUM(OL_AMOUNT) AS OL_TOTAL" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + "" + " WHERE OL_W_ID = ?" + " AND OL_D_ID = ?"
            + " AND OL_O_ID = ?";
    public static final String STMT_UPDATE_CUST_BAL_DELIVERY_COUNT = "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = ?" + ", C_DELIVERY_CNT = ?" + " WHERE C_W_ID = ?"
            + " AND C_D_ID = ?" + " AND C_ID = ?";
    public static final String STMT_GET_CUST_BAN = "SELECT C_BALANCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE C_W_ID = ?" + " AND C_D_ID = ?" + " AND C_ID = ?";
    public static final String STMT_GET_DELIVERY_COUNT = "SELECT C_DELIVERY_CNT FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE C_W_ID = ?" + " AND C_D_ID = ?" + " AND C_ID = ?";

    public void run(TransactionClient txnClient, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        numCQLRead = 0;
        numCQLWrite = 0;
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
        txnClient.setPrepareStatement(DELIVERY_GET_OL_NUMBER, STMT_GET_OL_NUMBER);
        txnClient.setPrepareStatement(DELIVERY_SUM_ORDER_AMOUNT, STMT_SUM_ORDER_AMOUNT);
        txnClient.setPrepareStatement(DELIVERY_UPDATE_CUST_BAL_DELIVERY_COUNT, STMT_UPDATE_CUST_BAL_DELIVERY_COUNT);
        txnClient.setPrepareStatement(DELIVERY_GET_CUST_BAN, STMT_GET_CUST_BAN);
        txnClient.setPrepareStatement(DELIVERY_GET_DELIVERY_COUNT, STMT_GET_DELIVERY_COUNT);
    }

    private int deliveryTransaction(int w_id, int o_carrier_id, TransactionClient txnClient, TPCCWorker w) {

        int d_id, c_id;
        float ol_total;
        int[] orderIDs;

        orderIDs = new int[10];
        ResultSet rs;
        Row resultRow;

        for (d_id = 1; d_id <= 10; d_id++) {
            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(DELIVERY_GET_ORDER_ID, String.valueOf(w_id), w_id, d_id));
            numCQLRead++;
            if (rs.isEmpty()) {
                // This district has no new orders; this can happen but should
                // be rare
                continue;
            }

            List<Row> allRows = Lists.newArrayList(rs.allRows());
            // Sort desc
            Collections.sort(allRows, new Comparator<Row>() {

                @Override
                public int compare(Row o1, Row o2) {
                    if (o1 == null && o2 == null) {
                        return 0;
                    } else if (o1 == null) {
                        return -1;
                    } else if (o2 == null) {
                        return 1;
                    } else {
                        return o1.getInt("NO_O_ID") - o2.getInt("NO_O_ID");
                    }
                }
            });

            resultRow = allRows.get(0);
            int no_o_id = resultRow.getInt("NO_O_ID");
            orderIDs[d_id - 1] = no_o_id;
            rs = null;

            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(DELIVERY_DELETE_NEW_ORDER, String.valueOf(w_id), w_id, d_id, no_o_id));
            numCQLWrite++;
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

            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(DELIVERY_GET_CUST_ID, String.valueOf(w_id), w_id, d_id, no_o_id));
            numCQLRead++;
            if (rs.isEmpty())
                throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");
            resultRow = rs.iterator().next();
            c_id = resultRow.getInt("O_C_ID");
            rs = null;

            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(DELIVERY_UPDATE_CARRIER_ID, String.valueOf(w_id), o_carrier_id, w_id, d_id, no_o_id));
            numCQLWrite++;
            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(DELIVERY_GET_OL_NUMBER, String.valueOf(w_id), w_id, d_id, no_o_id));
            numCQLRead++;
            for (Row row : rs.allRows()) {
                rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(DELIVERY_UPDATE_DELIVERY_DATE, String.valueOf(w_id), System.currentTimeMillis(), w_id, d_id, no_o_id,
                        row.getInt("OL_NUMBER")));
                numCQLWrite++;
            }

            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(DELIVERY_SUM_ORDER_AMOUNT, String.valueOf(w_id), w_id, d_id, no_o_id));
            numCQLRead++;
            if (rs.isEmpty())
                throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");
            resultRow = rs.iterator().next();
            ol_total = resultRow.getFloat("OL_TOTAL");
            rs = null;

            float cust_ban;
            int delivery_count;
            // Read before write!
            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(DELIVERY_GET_CUST_BAN, String.valueOf(w_id), w_id, d_id, c_id));
            numCQLRead++;
            if (rs.isEmpty())
                throw new RuntimeException("C_ID=" + c_id + " C_W_ID=" + w_id + " C_D_ID=" + d_id + " not found!");
            resultRow = rs.iterator().next();
            cust_ban = resultRow.getFloat("C_BALANCE");

            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(DELIVERY_GET_DELIVERY_COUNT, String.valueOf(w_id), w_id, d_id, c_id));
            numCQLRead++;
            if (rs.isEmpty())
                throw new RuntimeException("C_ID=" + c_id + " C_W_ID=" + w_id + " C_D_ID=" + d_id + " not found!");
            resultRow = rs.iterator().next();
            delivery_count = resultRow.getInt("C_DELIVERY_CNT");

            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(DELIVERY_UPDATE_CUST_BAL_DELIVERY_COUNT, String.valueOf(w_id), ol_total + cust_ban, delivery_count + 1, w_id, d_id,
                    c_id));
            numCQLWrite++;
        }

        int skippedDeliveries = 0;
        for (int i = 1; i <= 10; i++) {
            if (orderIDs[i - 1] < 0) {
                skippedDeliveries++;
            }
        }
        return skippedDeliveries;
    }
}
