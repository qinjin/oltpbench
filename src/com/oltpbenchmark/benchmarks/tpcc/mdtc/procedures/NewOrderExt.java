package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.util.Random;

import mdtc.api.transaction.client.ResultSet;
import mdtc.api.transaction.client.Row;
import mdtc.api.transaction.client.TransactionClient;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;

public class NewOrderExt extends MDTCProcedure {

    private static final Logger LOG = Logger.getLogger(NewOrderExt.class);

    public static final String NEWORDER_INSERT_ORDER_LINE_CQL = "NEWORDER_INSERT_ORDER_LINE_CQL";
    public static final String NEWORDER_UPDATE_STOCK_CQL = "NEWORDER_UPDATE_STOCK_CQL";
    public static final String NEWORDER_GET_STOCK_CQL = "NEWORDER_GET_STOCK_CQL";
    public static final String NEWORDER_GET_ITEM_CQL = "NEWORDER_GET_ITEM_CQL";
    public static final String NEWORDER_INSERT_ORDER_CQL = "NEWORDER_INSERT_ORDER_CQL";
    public static final String NEWORDER_UPDATE_DIST_CQL = "NEWORDER_UPDATE_DIST_CQL";
    public static final String NEWORDER_INSERT_NEW_ORDER_CQL = "NEWORDER_INSERT_NEW_ORDER_CQL";
    public static final String NEWORDER_GET_DIST_CQL = "NEWORDER_GET_DIST_CQL";
    public static final String NEWORDER_GET_WH_CQL = "NEWORDER_GET_WH_CQL";
    public static final String NEWORDER_GET_CUST_CQL = "NEWORDER_GET_CUST_CQL";
    public static final String NEWORDER_GET_NEXT_ORDER_ID = "NEWORDER_GET_NEXT_ORDER_ID";
    public static final String NEWORDER_GET_STOCK_YTD = "NEWORDER_GET_STOCK_YTD";
    public static final String NEWORDER_GET_ORDER_CNT = "NEWORDER_GET_ORDER_CNT";
    public static final String NEWORDER_GET_REMOTE_CNT = "NEWORDER_GET_REMOTE_CNT";

    public final String STMT_GET_CUST_CQL = "SELECT C_DISCOUNT, C_LAST, C_CREDIT" + "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";
    public final String STMT_GET_WH_CQL = "SELECT W_TAX" + "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE + " WHERE W_ID = ?";
    public final String STMT_GET_DIST_CQL = "SELECT D_NEXT_O_ID, D_TAX FROM " + TPCCConstants.TABLENAME_DISTRICT
                    + " WHERE D_W_ID = ?  AND D_ID = ?";
    public final String STMT_INSERT_NEW_ORDER_CQL = "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER + " (NO_O_ID, NO_D_ID, NO_W_ID) VALUES ( ?, ?, ?)";
    public final String STMT_UPDATE_DIST_CQL = "UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_NEXT_O_ID = ? WHERE D_W_ID = ? AND D_ID = ?";
    public final String STMT_INSERT_ORDER_CQL = "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER + " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?)";
    public final String STMT_GET_ITEM_CQL = "SELECT I_PRICE, I_NAME , I_DATA FROM " + TPCCConstants.TABLENAME_ITEM + " WHERE I_ID = ?";
    public final String STMT_GET_STOCK_CQL = "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" + " FROM "
            + TPCCConstants.TABLENAME_STOCK + " WHERE S_W_ID = ? AND S_I_ID = ?";
    public final String STMT_UPDATE_STOCK_CQL = "UPDATE " + TPCCConstants.TABLENAME_STOCK + " SET S_QUANTITY = ? , S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? " + " WHERE S_W_ID = ? AND S_I_ID = ?";
    public final String STMT_INSERT_ORDER_LINE_CQL = "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE + " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID,"
            + "  OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?,?,?,?,?,?,?,?,?)";
    public final String STMT_GET_NEXT_ORDER_ID = "SELECT D_NEXT_O_ID FROM " + TPCCConstants.TABLENAME_DISTRICT + " WHERE D_W_ID = ? AND D_ID = ?";
    public final String STMT_GET_STOCK_YTD = "SELECT S_YTD FROM " + TPCCConstants.TABLENAME_STOCK + " WHERE S_W_ID = ? AND S_I_ID = ?";
    public final String STMT_GET_ORDER_CNT = "SELECT S_ORDER_CNT FROM " + TPCCConstants.TABLENAME_STOCK + " WHERE S_W_ID = ? AND S_I_ID = ?";
    public final String STMT_GET_REMOTE_CNT = "SELECT S_REMOTE_CNT FROM " + TPCCConstants.TABLENAME_STOCK + " WHERE S_W_ID = ? AND S_I_ID = ?";

    public void run(TransactionClient txnClient, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        initStatements(txnClient);

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = TPCCUtil.getCustomerID(gen);

        int numItems = (int) TPCCUtil.randomNumber(5, 15, gen);
        int[] itemIDs = new int[numItems];
        int[] supplierWarehouseIDs = new int[numItems];
        int[] orderQuantities = new int[numItems];
        int allLocal = 1;
        for (int i = 0; i < numItems; i++) {
            itemIDs[i] = TPCCUtil.getItemID(gen);
            if (TPCCUtil.randomNumber(1, 100, gen) > 1) {
                supplierWarehouseIDs[i] = terminalWarehouseID;
            } else {
                do {
                    supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, numWarehouses, gen);
                } while (supplierWarehouseIDs[i] == terminalWarehouseID && numWarehouses > 1);
                allLocal = 0;
            }
            orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
        }

        // we need to cause 1% of the new orders to be rolled back.
        if (TPCCUtil.randomNumber(1, 100, gen) == 1)
            itemIDs[numItems - 1] = jTPCCConfig.INVALID_ITEM_ID;

        newOrderTransaction(terminalWarehouseID, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, txnClient, w);

    }

    public void initStatements(TransactionClient txnClient) {
        txnClient.setPrepareStatement(NEWORDER_GET_WH_CQL, STMT_GET_WH_CQL);
        txnClient.setPrepareStatement(NEWORDER_GET_CUST_CQL, STMT_GET_CUST_CQL);
        txnClient.setPrepareStatement(NEWORDER_GET_DIST_CQL, STMT_GET_DIST_CQL);
        txnClient.setPrepareStatement(NEWORDER_INSERT_NEW_ORDER_CQL, STMT_INSERT_NEW_ORDER_CQL);
        txnClient.setPrepareStatement(NEWORDER_UPDATE_DIST_CQL, STMT_UPDATE_DIST_CQL);
        txnClient.setPrepareStatement(NEWORDER_INSERT_ORDER_CQL, STMT_INSERT_ORDER_CQL);
        txnClient.setPrepareStatement(NEWORDER_GET_ITEM_CQL, STMT_GET_ITEM_CQL);
        txnClient.setPrepareStatement(NEWORDER_GET_STOCK_CQL, STMT_GET_STOCK_CQL);
        txnClient.setPrepareStatement(NEWORDER_UPDATE_STOCK_CQL, STMT_UPDATE_STOCK_CQL);
        txnClient.setPrepareStatement(NEWORDER_INSERT_ORDER_LINE_CQL, STMT_INSERT_ORDER_LINE_CQL);
        txnClient.setPrepareStatement(NEWORDER_GET_NEXT_ORDER_ID, STMT_GET_NEXT_ORDER_ID);
        txnClient.setPrepareStatement(NEWORDER_GET_STOCK_YTD, STMT_GET_STOCK_YTD);
        txnClient.setPrepareStatement(NEWORDER_GET_ORDER_CNT, STMT_GET_ORDER_CNT);
        txnClient.setPrepareStatement(NEWORDER_GET_REMOTE_CNT, STMT_GET_REMOTE_CNT);
    }

    private void newOrderTransaction(int w_id, int d_id, int c_id, int o_ol_cnt, int o_all_local, int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities, TransactionClient txnClient,
            TPCCWorker w) {
        float c_discount, w_tax, d_tax = 0, i_price;
        int d_next_o_id, o_id = -1, s_quantity;
        String c_last = null, c_credit = null, i_name, i_data, s_data;
        String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
        String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, ol_dist_info = null;
        float[] itemPrices = new float[o_ol_cnt];
        float[] orderLineAmounts = new float[o_ol_cnt];
        String[] itemNames = new String[o_ol_cnt];
        int[] stockQuantities = new int[o_ol_cnt];
        char[] brandGeneric = new char[o_ol_cnt];
        int ol_supply_w_id, ol_i_id, ol_quantity;
        int s_remote_cnt_increment;
        float ol_amount, total_amount = 0;

        ResultSet rs;
        Row resultRow;
        try {
            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_GET_WH_CQL, String.valueOf(w_id), w_id));
            if (rs.isEmpty())
                throw new RuntimeException("W_ID=" + w_id + " C_D_ID=" + d_id + " C_ID=" + c_id + " not found!");
            resultRow = rs.iterator().next();
            c_discount = resultRow.getFloat("C_DISCOUNT");
            c_last = resultRow.getString("C_LAST");
            c_credit = resultRow.getString("C_CREDIT");
            
            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_GET_CUST_CQL, String.valueOf(w_id), w_id, d_id, c_id));
            if (rs.isEmpty())
                throw new RuntimeException("W_ID=" + w_id + " C_D_ID=" + d_id + " C_ID=" + c_id + " not found!");
            resultRow = rs.iterator().next();
            w_tax = resultRow.getFloat("W_TAX");
            rs = null;

            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_GET_DIST_CQL, String.valueOf(w_id), w_id, d_id));
            if (rs.isEmpty()) {
                throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
            }
            resultRow = rs.iterator().next();
            d_next_o_id = resultRow.getInt("D_NEXT_O_ID");
            d_tax = resultRow.getFloat("D_TAX");
            rs = null;

            int next_oder_id;
            // Read before write
            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_GET_NEXT_ORDER_ID, String.valueOf(w_id), w_id, d_id));
            if (rs.isEmpty())
                throw new RuntimeException("Error!! Cannot get next_order_id on district for D_ID=" + d_id + " D_W_ID=" + w_id);
            resultRow = rs.iterator().next();
            next_oder_id = resultRow.getInt("D_NEXT_O_ID") + 1;

            // woonhak, need to change order because of foreign key constraints
            // update next_order_id first, but it might doesn't matter
            // statement = new BoundStatement(stmtUpdateDist).bind(1,
            // w_id).bind(2, w_id);
            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_UPDATE_DIST_CQL, String.valueOf(w_id), next_oder_id, w_id, d_id));

            o_id = d_next_o_id;

            // woonhak, need to change order, because of foreign key constraints
            // [[insert ooder first
            // statement = new BoundStatement(stmtInsertOOrder).bind(1,
            // w_id).bind(2, w_id).bind(3, w_id).bind(4, w_id).bind(6,
            // o_ol_cnt).bind(7, o_all_local);
            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_INSERT_ORDER_CQL, String.valueOf(w_id), o_id, d_id, w_id, c_id, System.currentTimeMillis(), o_ol_cnt, o_all_local));
            // stmtInsertOOrder.setTimestamp(5, new
            // Timestamp(System.currentTimeMillis()));
            // insert ooder first]]
            /* TODO: add error checking */

            // statement = new BoundStatement(stmtInsertNewOrder).bind(1,
            // o_id).bind(2, d_id).bind(3, w_id);
            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_INSERT_NEW_ORDER_CQL, String.valueOf(w_id), o_id, d_id, w_id));

            /*
             * woonhak, [[change order stmtInsertOOrder.setInt(1, o_id);
             * stmtInsertOOrder.setInt(2, d_id); stmtInsertOOrder.setInt(3,
             * w_id); stmtInsertOOrder.setInt(4, c_id);
             * stmtInsertOOrder.setTimestamp(5, new
             * Timestamp(System.currentTimeMillis()));
             * stmtInsertOOrder.setInt(6, o_ol_cnt); stmtInsertOOrder.setInt(7,
             * o_all_local); stmtInsertOOrder.executeUpdate(); change order]]
             */

            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
                ol_i_id = itemIDs[ol_number - 1];
                ol_quantity = orderQuantities[ol_number - 1];
                // statement = new BoundStatement(stmtGetItem).bind(1, ol_i_id);
                rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_GET_ITEM_CQL, String.valueOf(ol_i_id), ol_i_id));
                if (rs.isEmpty()) {
                    // This is (hopefully) an expected error: this is an
                    // expected new order rollback
                    assert ol_number == o_ol_cnt;
                    assert ol_i_id == jTPCCConfig.INVALID_ITEM_ID;
                    throw new UserAbortException("EXPECTED new order rollback: I_ID=" + ol_i_id + " not found!");
                }

                resultRow = rs.iterator().next();
                i_price = resultRow.getFloat("I_PRICE");
                i_name = resultRow.getString("I_NAME");
                i_data = resultRow.getString("I_DATA");
                rs = null;

                itemPrices[ol_number - 1] = i_price;
                itemNames[ol_number - 1] = i_name;

                // statement = new BoundStatement(stmtGetStock).bind(1,
                // ol_i_id).bind(2, ol_supply_w_id);
                rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_GET_STOCK_CQL, String.valueOf(ol_supply_w_id), ol_supply_w_id, ol_i_id));
                if (!rs.iterator().hasNext())
                    throw new RuntimeException("I_ID=" + ol_i_id + " not found!");

                resultRow = rs.iterator().next();
                s_quantity = resultRow.getInt("S_QUANTITY");
                s_data = resultRow.getString("S_DATA");
                s_dist_01 = resultRow.getString("S_DIST_01");
                s_dist_02 = resultRow.getString("S_DIST_02");
                s_dist_03 = resultRow.getString("S_DIST_03");
                s_dist_04 = resultRow.getString("S_DIST_04");
                s_dist_05 = resultRow.getString("S_DIST_05");
                s_dist_06 = resultRow.getString("S_DIST_06");
                s_dist_07 = resultRow.getString("S_DIST_07");
                s_dist_08 = resultRow.getString("S_DIST_08");
                s_dist_09 = resultRow.getString("S_DIST_09");
                s_dist_10 = resultRow.getString("S_DIST_10");
                rs = null;

                stockQuantities[ol_number - 1] = s_quantity;

                if (s_quantity - ol_quantity >= 10) {
                    s_quantity -= ol_quantity;
                } else {
                    s_quantity += -ol_quantity + 91;
                }

                if (ol_supply_w_id == w_id) {
                    s_remote_cnt_increment = 0;
                } else {
                    s_remote_cnt_increment = 1;
                }

                float stock_ytd;
                int oder_cnt;
                int remote_cnt;
                // Read before write!
                rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_GET_STOCK_YTD, String.valueOf(ol_supply_w_id), ol_supply_w_id, ol_i_id));
                if (rs.isEmpty()) {
                    throw new RuntimeException("S_I_ID=" + ol_i_id + " S_W_ID=" + ol_supply_w_id + " not found!");
                }
                resultRow = rs.iterator().next();
                stock_ytd = resultRow.getFloat("S_YTD");

                rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_GET_ORDER_CNT, String.valueOf(ol_supply_w_id), ol_supply_w_id, ol_i_id));
                if (rs.isEmpty()) {
                    throw new RuntimeException("S_I_ID=" + ol_i_id + " S_W_ID=" + ol_supply_w_id + " not found!");
                }
                resultRow = rs.iterator().next();
                oder_cnt = resultRow.getInt("S_ORDER_CNT");

                rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_GET_REMOTE_CNT, String.valueOf(ol_supply_w_id), ol_supply_w_id, ol_i_id));
                if (rs.isEmpty()) {
                    throw new RuntimeException("S_I_ID=" + ol_i_id + " S_W_ID=" + ol_supply_w_id + " not found!");
                }
                resultRow = rs.iterator().next();
                remote_cnt = resultRow.getInt("S_REMOTE_CNT");

                rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_UPDATE_STOCK_CQL, String.valueOf(ol_supply_w_id), s_quantity, ol_quantity + stock_ytd, oder_cnt + 1, s_remote_cnt_increment + remote_cnt, ol_supply_w_id, ol_i_id));

                ol_amount = ol_quantity * i_price;
                orderLineAmounts[ol_number - 1] = ol_amount;
                total_amount += ol_amount;

                if (i_data.indexOf("GENERIC") != -1 && s_data.indexOf("GENERIC") != -1) {
                    brandGeneric[ol_number - 1] = 'B';
                } else {
                    brandGeneric[ol_number - 1] = 'G';
                }

                switch ((int) d_id) {
                    case 1:
                        ol_dist_info = s_dist_01;
                        break;
                    case 2:
                        ol_dist_info = s_dist_02;
                        break;
                    case 3:
                        ol_dist_info = s_dist_03;
                        break;
                    case 4:
                        ol_dist_info = s_dist_04;
                        break;
                    case 5:
                        ol_dist_info = s_dist_05;
                        break;
                    case 6:
                        ol_dist_info = s_dist_06;
                        break;
                    case 7:
                        ol_dist_info = s_dist_07;
                        break;
                    case 8:
                        ol_dist_info = s_dist_08;
                        break;
                    case 9:
                        ol_dist_info = s_dist_09;
                        break;
                    case 10:
                        ol_dist_info = s_dist_10;
                        break;
                }

                // statement = new BoundStatement(stmtInsertOrderLine).bind(1,
                // o_id).bind(2, d_id).bind(3, w_id).bind(4, ol_number).bind(5,
                // ol_i_id).bind(6, ol_supply_w_id).bind(7, ol_quantity)
                // .bind(8, ol_amount).bind(9, ol_dist_info);
                rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_INSERT_ORDER_LINE_CQL, String.valueOf(w_id), o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity,ol_amount,ol_dist_info));

            } // end-for

            total_amount *= (1 + w_tax + d_tax) * (1 - c_discount);
        } catch (UserAbortException userEx) {
            LOG.debug("Caught an expected error in New Order");
            throw userEx;
        } finally {
        }
    }

}
