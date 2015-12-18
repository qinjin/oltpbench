package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.log4j.Logger;

import mdtc.api.transaction.client.ResultSet;
import mdtc.api.transaction.client.TransactionClient;
import mdtc.api.transaction.client.TxnStatement;
import mdtc.api.transaction.data.IsolationLevel;
import mdtc.impl.APIFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class BatchedNewOrderExt extends NewOrderExt {
    private static final Logger LOG = Logger.getLogger(NewOrderExt.class);

    private static final AtomicInteger ORDER_ID = new AtomicInteger();
    private static final Random r = new Random();

    int txnType = 0;
    final boolean disableZipf;
    ZipfDistribution zipf;
    
    public BatchedNewOrderExt() {
        txnType = APIFactory.getTxnType();
        double zipfExponent = APIFactory.zipfExponent();
        disableZipf = Double.valueOf(zipfExponent).equals(Double.valueOf(0));
        zipf = disableZipf ? null : new ZipfDistribution(10000, zipfExponent);
    }

    @Override
    public void run(TransactionClient txnClient, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        numCQLRead = 0;
        numCQLWrite = 0;
        numSucceed = 0;
        numAborted = 0;
        latency = 0;
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

        batchecNewOrderTransaction(gen, terminalWarehouseID, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, txnClient, w);
    }

    private void batchecNewOrderTransaction(Random gen, int w_id, int d_id, int c_id, int o_ol_cnt, int o_all_local, int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities,
            TransactionClient txnClient, TPCCWorker w) {
        try {
            
            int o_id = disableZipf ? r.nextInt(100000) : zipf.sample();
//            int o_id = ORDER_ID.getAndIncrement();
            
//            System.out.println(o_id);
            
            TxnStatement statement1 = MDTCUtil.buildPreparedStatement(true, NEWORDER_GET_DIST_CQL, String.valueOf(w_id), w_id);
            TxnStatement statement2 = MDTCUtil.buildPreparedStatement(true, NEWORDER_GET_OPEN_ORDER, String.valueOf(o_id), w_id, d_id, o_id);
            TxnStatement statement3 = MDTCUtil.buildPreparedStatement(true, NEWORDER_GET_NEW_ORDER, String.valueOf(o_id), w_id, d_id, o_id);
            TxnStatement statement4 = MDTCUtil.buildPreparedStatement(false, NEWORDER_UPDATE_DIST_CQL, String.valueOf(w_id), o_id, w_id, d_id);
            TxnStatement statement5 = MDTCUtil
                    .buildPreparedStatement(false, NEWORDER_INSERT_ORDER_CQL, String.valueOf(o_id), o_id, d_id, w_id, c_id, System.currentTimeMillis(), o_ol_cnt, o_all_local);
            TxnStatement statement6 = MDTCUtil.buildPreparedStatement(false, NEWORDER_INSERT_NEW_ORDER_CQL, String.valueOf(o_id), o_id, d_id, w_id);

            // Read-only txns
            List<TxnStatement> allReadStatements = Lists.newArrayList(statement1, statement2, statement3);
            // Write-only txns
            List<TxnStatement> allWriteStatements = Lists.newArrayList(statement4, statement5, statement6);
            // Write-Read txns
            List<TxnStatement> allStatements = Lists.newArrayList(statement4, statement1, statement5, statement2, statement6, statement3);
            // Selected statements.
            List<TxnStatement> statements = Lists.newArrayList();
            // Number of statements per transaction: 20%:1, 20%2, 60%:3.
            int numStatments;
            float randomFloat = gen.nextFloat();
            if (randomFloat < 0.2f) {
                numStatments = 1;
            } else if (randomFloat < 0.8f) {
                numStatments = 3;
            } else {
                numStatments = 2;
            }
            switch (txnType) {
                case 0:
                    for (int i = 0; i < numStatments; i++) {
                        statements.add(allReadStatements.get(i));
                    }
                    numCQLRead += numStatments;
                    break;
                case 1:
                    for (int i = 0; i < numStatments; i++) {
                        statements.add(allWriteStatements.get(i));
                    }
                    numCQLWrite += numStatments;
                    break;
                case 2:
                    // 20% read-only, 80% read-write
                    if (gen.nextFloat() < 0.2f) {
                        for (int i = 0; i < numStatments; i++) {
                            statements.add(allReadStatements.get(i));
                        }
                        numCQLRead += numStatments;
                    } else {
                        for (int i = 0; i < numStatments; i++) {
                            TxnStatement stmt = allStatements.get(i);
                            if (stmt.isRead) {
                                numCQLRead++;
                            } else {
                                numCQLWrite++;
                            }
                            statements.add(stmt);
                        }
                    }
                    break;
                case 3:
                    // 80% read-only, 20% write
                    if (gen.nextFloat() < 0.8f) {
                        for (int i = 0; i < numStatments; i++) {
                            statements.add(allReadStatements.get(i));
                        }
                        numCQLRead += numStatments;
                    } else {
                        for (int i = 0; i < numStatments; i++) {
                            TxnStatement stmt = allStatements.get(i);
                            if (stmt.isRead) {
                                numCQLRead++;
                            } else {
                                numCQLWrite++;
                            }
                            statements.add(stmt);
                        }
                    }
                    break;
                default:
                    if (gen.nextFloat() < 0.2f) {
                        for (int i = 0; i < numStatments; i++) {
                            statements.add(allReadStatements.get(i));
                        }
                        numCQLRead += numStatments;
                    } else {
                        for (int i = 0; i < numStatments; i++) {
                            TxnStatement stmt = allStatements.get(i);
                            if (stmt.isRead) {
                                numCQLRead++;
                            } else {
                                numCQLWrite++;
                            }
                            statements.add(stmt);
                        }
                    }
                    break;
            }

            ResultSet result = txnClient.executeMultiStatementsTxn(IsolationLevel.OneCopySerilizible, statements);
            if (result.isSucceed()) {
                numSucceed++;
                latency = result.latency;
            } else {
                numAborted++;
            }
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