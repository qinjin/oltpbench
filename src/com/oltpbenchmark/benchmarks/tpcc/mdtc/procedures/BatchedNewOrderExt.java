package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import mdtc.api.transaction.client.TransactionClient;
import mdtc.api.transaction.client.TxnStatement;
import mdtc.api.transaction.data.IsolationLevel;
import mdtc.impl.APIFactory;

import com.google.common.collect.Lists;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class BatchedNewOrderExt extends NewOrderExt {
    private static final Logger LOG = Logger.getLogger(NewOrderExt.class);

    private static final AtomicInteger ORDER_ID = new AtomicInteger();
    
    int txnType = 0;
    
    public BatchedNewOrderExt() {
        txnType = APIFactory.getTxnType();
    }

    @Override
    public void run(TransactionClient txnClient, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        numCQLRead = 0;
        numCQLWrite = 0;
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
        try {
            int o_id  = ORDER_ID.getAndIncrement();
            TxnStatement statement1 = MDTCUtil.buildPreparedStatement(NEWORDER_GET_WH_CQL, String.valueOf(w_id), w_id);
            TxnStatement statement2 = MDTCUtil.buildPreparedStatement(NEWORDER_GET_CUST_CQL, String.valueOf(w_id), w_id, d_id, c_id);
            TxnStatement statement3 = MDTCUtil.buildPreparedStatement(NEWORDER_GET_DIST_CQL, String.valueOf(w_id), w_id, d_id);
            TxnStatement statement4 = MDTCUtil.buildPreparedStatement(NEWORDER_UPDATE_DIST_CQL, String.valueOf(o_id), o_id, w_id, d_id);
            TxnStatement statement5 = MDTCUtil.buildPreparedStatement(NEWORDER_INSERT_ORDER_CQL, String.valueOf(o_id), o_id, d_id, w_id, c_id, System.currentTimeMillis(), o_ol_cnt, o_all_local);
            TxnStatement statement6 = MDTCUtil.buildPreparedStatement(NEWORDER_INSERT_NEW_ORDER_CQL, String.valueOf(o_id), o_id, d_id, w_id);
            
            List<TxnStatement> allStatements = Lists.newArrayList();

            switch (txnType) {
                case 0:
                    allStatements.add(statement1);
                    allStatements.add(statement2);
                    allStatements.add(statement3);
                    numCQLRead += 3;
                    break;
                case 1:
                    allStatements.add(statement4);
                    allStatements.add(statement5);
                    allStatements.add(statement6);
                    numCQLWrite += 3;
                    break;
                case 2:
                    allStatements.add(statement1);
                    allStatements.add(statement5);
                    allStatements.add(statement6);
                    numCQLRead += 1;
                    numCQLWrite += 2;
                    break;
                case 3:
                    allStatements.add(statement1);
                    allStatements.add(statement2);
                    allStatements.add(statement5);
                    numCQLRead += 2;
                    numCQLWrite += 1;
                    break;
                default:
                    allStatements.add(statement1);
                    allStatements.add(statement5);
                    allStatements.add(statement6);
                    numCQLRead += 1;
                    numCQLWrite += 2;
                    break;
            }

            txnClient.executeMultiStatementsTxn(IsolationLevel.OneCopySerilizible, allStatements);
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