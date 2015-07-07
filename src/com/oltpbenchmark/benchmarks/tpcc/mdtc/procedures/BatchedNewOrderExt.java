package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.util.Random;

import org.apache.log4j.Logger;

import mdtc.api.transaction.client.ResultSet;
import mdtc.api.transaction.client.Row;
import mdtc.api.transaction.client.TransactionClient;

import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class BatchedNewOrderExt extends NewOrderExt {
    private static final Logger LOG = Logger.getLogger(NewOrderExt.class);

    @Override
    public void run(TransactionClient txnClient, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        initStatements(txnClient);

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = TPCCUtil.getCustomerID(gen);

        int numItems = (int) TPCCUtil.randomNumber(5, 15, gen);
        // No invalid items, skip roll back.
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

        batchecNewOrderTransaction(terminalWarehouseID, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, txnClient, w);
    }

    private void batchecNewOrderTransaction(int w_id, int d_id, int c_id, int o_ol_cnt, int o_all_local, int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities, TransactionClient txnClient,
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

            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_UPDATE_DIST_CQL, String.valueOf(w_id), next_oder_id, w_id, d_id));

            o_id = d_next_o_id;
            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_INSERT_ORDER_CQL, String.valueOf(w_id), o_id, d_id, w_id, c_id, System.currentTimeMillis(), o_ol_cnt, o_all_local));

            rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(NEWORDER_INSERT_NEW_ORDER_CQL, String.valueOf(w_id), o_id, d_id, w_id));
            
        } catch (UserAbortException userEx) {
            LOG.debug("Caught an expected error in New Order");
            throw userEx;
        } finally {
        }
    }

    @Override
    public void initStatements(TransactionClient txnClient) {
        super.initStatements(txnClient);
    }

}