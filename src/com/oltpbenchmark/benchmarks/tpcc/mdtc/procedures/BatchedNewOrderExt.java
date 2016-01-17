package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import mdtc.api.transaction.client.ResultSet;
import mdtc.api.transaction.client.TransactionClient;
import mdtc.api.transaction.client.TxnStatement;
import mdtc.api.transaction.data.IsolationLevel;
import mdtc.conf.TranConfReader.EvaluationType;
import mdtc.impl.APIFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.io.Files;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class BatchedNewOrderExt extends NewOrderExt {
    private static final Logger LOG = Logger.getLogger(BatchedNewOrderExt.class);

    private static final Random r = new Random();
    private static final int RANDOM_KEY_SPACE = 100000;
    
    private static final LinkedBlockingQueue<Integer> zipfOID = Queues.newLinkedBlockingQueue();
    
    public static final List<String> allKeys = Lists.newArrayList();
    
    public static void initZipf(){
        double zipfExponent = APIFactory.zipfExponent();
        boolean disableZipf = Double.valueOf(zipfExponent).equals(Double.valueOf(0));
        if(!disableZipf){
            File f = new File("/home/qinjin/mdtc/oltpbench/zipf_100000.txt");
            if(!f.exists()){
                throw new RuntimeException("Zipf data file zipf_100000.txt does not exist!");
            }
            
            System.out.println("Started to load zipf from file "+f);
            try {
                loadZipfFromFile(f, zipfOID);
            } catch (Exception e) {
                LOG.error("Could not load zipf from file "+ f+ " Exception: "+e.getMessage());
                e.printStackTrace();
            }
            System.out.println("Loaded "+zipfOID.size()+" zipf from file.");
        } else {
            System.out.println("Zipf is disabled!");
        }
    }

    //Load zipf from a comma separated file.
    private static void loadZipfFromFile(File f, LinkedBlockingQueue<Integer> zipfoid) throws IOException {
        List<String> lines = Files.readLines(f, Charset.defaultCharset());
        for(String line : lines){
            String[] zipfs = line.split(",");
            for(String zipfStr : zipfs){
                Integer zipf = Integer.parseInt(zipfStr);
                zipfoid.add(zipf);
            }
        }
    }


    int txnType = 0;
    final boolean disableZipf;
    
    public BatchedNewOrderExt() {
        txnType = APIFactory.getTxnType();
        double zipfExponent = APIFactory.zipfExponent();
        disableZipf = Double.valueOf(zipfExponent).equals(Double.valueOf(0));
        r.setSeed(System.currentTimeMillis() + APIFactory.getDatacenterID() * 1001);
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
            
            int o_id = disableZipf ? r.nextInt(RANDOM_KEY_SPACE) : zipfOID.take();
            TxnStatement statement1 = MDTCUtil.buildPreparedStatement(true, NEWORDER_GET_DIST_CQL, String.valueOf(o_id), w_id);
            TxnStatement statement2 = MDTCUtil.buildPreparedStatement(true, NEWORDER_GET_OPEN_ORDER, String.valueOf(o_id), w_id, d_id, o_id);
            TxnStatement statement3 = MDTCUtil.buildPreparedStatement(true, NEWORDER_GET_NEW_ORDER, String.valueOf(o_id), w_id, d_id, o_id);
            TxnStatement statement4 = MDTCUtil.buildPreparedStatement(false, NEWORDER_UPDATE_DIST_CQL, String.valueOf(o_id), o_id, w_id, d_id);
            TxnStatement statement5 = MDTCUtil
                    .buildPreparedStatement(false, NEWORDER_INSERT_ORDER_CQL, String.valueOf(o_id), o_id, d_id, w_id, c_id, System.currentTimeMillis(), o_ol_cnt, o_all_local);
            TxnStatement statement6 = MDTCUtil.buildPreparedStatement(false, NEWORDER_INSERT_NEW_ORDER_CQL, String.valueOf(o_id), o_id, d_id, w_id);

            // Read-only txns
            List<TxnStatement> allReadStatements = Lists.newArrayList(statement1, statement2, statement3);
            // Write-only txns
            List<TxnStatement> allWriteStatements = Lists.newArrayList(statement4, statement5, statement6);
            // Write-Read txns
            List<TxnStatement> allStatements = Lists.newArrayList(statement4, statement1, statement5, statement2, statement6, statement3);
            if(APIFactory.getEvaluationType() != EvaluationType.MDTC){
                Collections.shuffle(allStatements);
            }
            // Selected statements.
            List<TxnStatement> statements = Lists.newArrayList();
            // Number of statements per transaction: 20%:1, 20%2, 60%:3.
            int numStatments;
            float randomFloat = gen.nextFloat();
            if (randomFloat < 0.2f) {
                numStatments = 1;
            } else if (randomFloat > 0.8f) {
                numStatments = 2;
            } else {
                numStatments = 3;
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
        } catch (InterruptedException e) {
            LOG.debug("Caught an expected error in New Order");
            e.printStackTrace();
        } finally {
        }
    }

    @Override
    public void initStatements(TransactionClient txnClient) {
        super.initStatements(txnClient);
    }
}