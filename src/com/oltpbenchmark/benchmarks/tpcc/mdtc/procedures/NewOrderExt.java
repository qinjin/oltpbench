package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.util.Random;

import org.apache.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import com.datastax.driver.core.PreparedStatement;

public class NewOrderExt extends MDTCProcedure {

    private static final Logger LOG = Logger.getLogger(NewOrderExt.class);

    public final String STMT_GET_CUST_WH_CQL = "SELECT C_DISCOUNT, C_LAST, C_CREDIT, W_TAX" + "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + ", " + TPCCConstants.TABLENAME_WAREHOUSE
            + " WHERE W_ID = ? AND C_W_ID = ?" + " AND C_D_ID = ? AND C_ID = ?";
    public final String STMT_GET_DIST_CQL = "SELECT C_DISCOUNT, C_LAST, C_CREDIT, W_TAX" + "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + ", " + TPCCConstants.TABLENAME_WAREHOUSE
            + " WHERE W_ID = ? AND C_W_ID = ?" + " AND C_D_ID = ? AND C_ID = ?";
    public final String STMT_INSERT_NEW_ORDER_CQL = "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER + " (NO_O_ID, NO_D_ID, NO_W_ID) VALUES ( ?, ?, ?)";
    public final String STMT_UPDATE_DIST_CQL = "UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = ? AND D_ID = ?";
    public final String STMT_INSERT_ORDER_CQL = "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER + " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?)";
    public final String STMT_GET_ITEM_CQL = "SELECT I_PRICE, I_NAME , I_DATA FROM " + TPCCConstants.TABLENAME_ITEM + " WHERE I_ID = ?";
    public final String STMT_GET_STOCK_CQL = "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " + "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10"
            + " FROM " + TPCCConstants.TABLENAME_STOCK + " WHERE S_I_ID = ? AND S_W_ID = ? FOR UPDATE";
    public final String STMT_UPDATE_STOCK_CQL = "UPDATE " + TPCCConstants.TABLENAME_STOCK + " SET S_QUANTITY = ? , S_YTD = S_YTD + ?, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + ? "
            + " WHERE S_I_ID = ? AND S_W_ID = ?";
    public final String STMT_INSERT_ORDER_LINE_CQL = "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE + " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID,"
            + "  OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?,?,?,?,?,?,?,?,?)";

    private PreparedStatement stmtGetCustWhse = null;
    private PreparedStatement stmtGetDist = null;
    private PreparedStatement stmtInsertNewOrder = null;
    private PreparedStatement stmtUpdateDist = null;
    private PreparedStatement stmtInsertOOrder = null;
    private PreparedStatement stmtGetItem = null;
    private PreparedStatement stmtGetStock = null;
    private PreparedStatement stmtUpdateStock = null;
    private PreparedStatement stmtInsertOrderLine = null;

    public Result run(Session session, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        initStatements(session);

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

        newOrderTransaction(terminalWarehouseID, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, session, w);
        return null;

    }

    public void initStatements(Session session) {
        stmtGetCustWhse = session.prepare(STMT_GET_CUST_WH_CQL);
        stmtGetDist = session.prepare(STMT_GET_DIST_CQL);
        stmtInsertNewOrder = session.prepare(STMT_INSERT_NEW_ORDER_CQL);
        stmtUpdateDist = session.prepare(STMT_UPDATE_DIST_CQL);
        stmtInsertOOrder = session.prepare(STMT_INSERT_ORDER_CQL);
        stmtGetItem = session.prepare(STMT_GET_ITEM_CQL);
        stmtGetStock = session.prepare(STMT_GET_STOCK_CQL);
        stmtUpdateStock = session.prepare(STMT_UPDATE_STOCK_CQL);
        stmtInsertOrderLine = session.prepare(STMT_INSERT_ORDER_LINE_CQL);
    }

    private void newOrderTransaction(int w_id, int d_id, int c_id, int o_ol_cnt, int o_all_local, int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities, Session session, TPCCWorker w) {
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

        BoundStatement statement;
        ResultSet rs;
        Row resultRow;
        try {
            statement = new BoundStatement(stmtGetCustWhse).bind(1, w_id).bind(2, w_id).bind(3, w_id).bind(4, w_id);
            rs = session.execute(statement);
            if (!rs.iterator().hasNext())
                throw new RuntimeException("W_ID=" + w_id + " C_D_ID=" + d_id + " C_ID=" + c_id + " not found!");
            resultRow = rs.iterator().next();
            c_discount = resultRow.getFloat("C_DISCOUNT");
            c_last = resultRow.getString("C_LAST");
            c_credit = resultRow.getString("C_CREDIT");
            w_tax = resultRow.getFloat("W_TAX");
            rs = null;

            statement = new BoundStatement(stmtGetDist).bind(1, w_id).bind(2, w_id);
            rs = session.execute(statement);
            if (!rs.iterator().hasNext()) {
                throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
            }
            resultRow = rs.iterator().next();
            d_next_o_id = resultRow.getInt("D_NEXT_O_ID");
            d_tax = resultRow.getFloat("D_TAX");
            rs = null;

            // woonhak, need to change order because of foreign key constraints
            // update next_order_id first, but it might doesn't matter
            statement = new BoundStatement(stmtUpdateDist).bind(1, w_id).bind(2, w_id);
            rs = session.execute(statement);
            if (!rs.iterator().hasNext())
                throw new RuntimeException("Error!! Cannot update next_order_id on district for D_ID=" + d_id + " D_W_ID=" + w_id);

            o_id = d_next_o_id;

            // woonhak, need to change order, because of foreign key constraints
            // [[insert ooder first
            statement = new BoundStatement(stmtInsertOOrder).bind(1, w_id).bind(2, w_id).bind(3, w_id).bind(4, w_id).bind(6, o_ol_cnt).bind(7, o_all_local);
            rs = session.execute(statement);
            // TODO: Timestamp in Cassandra?????
            // stmtInsertOOrder.setTimestamp(5, new
            // Timestamp(System.currentTimeMillis()));
            // insert ooder first]]
            /* TODO: add error checking */

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
                statement = new BoundStatement(stmtGetItem).bind(1, ol_i_id);
                rs = session.execute(statement);
                if (!rs.iterator().hasNext()) {
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

                statement = new BoundStatement(stmtGetStock).bind(1, ol_i_id).bind(2, ol_supply_w_id);
                rs = session.execute(statement);
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

                statement = new BoundStatement(stmtUpdateStock).bind(1, s_quantity).bind(2, ol_quantity).bind(3, s_remote_cnt_increment).bind(4, ol_i_id).bind(5, ol_supply_w_id);
                rs = session.execute(statement);

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

                statement = new BoundStatement(stmtInsertOrderLine).bind(1, o_id).bind(2, d_id).bind(3, w_id).bind(4, ol_number).bind(5, ol_i_id).bind(6, ol_supply_w_id).bind(7, ol_quantity)
                        .bind(8, ol_amount).bind(9, ol_dist_info);
                rs = session.execute(statement);

            } // end-for

            total_amount *= (1 + w_tax + d_tax) * (1 - c_discount);
        } catch (UserAbortException userEx) {
            LOG.debug("Caught an expected error in New Order");
            throw userEx;
        } finally {
        }
    }

}
