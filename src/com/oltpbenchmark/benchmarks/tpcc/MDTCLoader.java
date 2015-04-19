package com.oltpbenchmark.benchmarks.tpcc;

import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.configCustPerDist;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.configDistPerWhse;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.configItemCount;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpcc.pojo.District;
import com.oltpbenchmark.benchmarks.tpcc.pojo.History;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Item;
import com.oltpbenchmark.benchmarks.tpcc.pojo.NewOrder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Oorder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.OrderLine;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Stock;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Warehouse;

public class MDTCLoader extends TPCCLoader {
    private static final Logger LOG = Logger.getLogger(MDTCLoader.class);

    public MDTCLoader(TPCCBenchmark benchmark, Connection c) {
        super(benchmark, c);
    }

    @Override
    public void load() throws SQLException {
        LOG.info("Start loading data to MDTC..");
        LOG.debug("Clearout the tables");
        // Clearout the tables
        truncateTable(TPCCConstants.TABLENAME_ITEM);
        truncateTable(TPCCConstants.TABLENAME_WAREHOUSE);
        truncateTable(TPCCConstants.TABLENAME_STOCK);
        truncateTable(TPCCConstants.TABLENAME_DISTRICT);
        truncateTable(TPCCConstants.TABLENAME_CUSTOMER);
        truncateTable(TPCCConstants.TABLENAME_HISTORY);
        truncateTable(TPCCConstants.TABLENAME_OPENORDER);
        truncateTable(TPCCConstants.TABLENAME_ORDERLINE);
        truncateTable(TPCCConstants.TABLENAME_NEWORDER);
        // seed the random number generator
        gen = new Random(System.currentTimeMillis());

        long totalRows = loadWhse(numWarehouses);
        totalRows += loadItem(configItemCount);
        totalRows += loadStock(numWarehouses, configItemCount);
        totalRows += loadDist(numWarehouses, configDistPerWhse);
        totalRows += loadCust(numWarehouses, configDistPerWhse, configCustPerDist);
        totalRows += loadOrder(numWarehouses, configDistPerWhse, configCustPerDist);
        LOG.info("Done loading data, " + totalRows + " rows loaded to MDTC.");
    }

    protected void truncateTable(String strTable) {
        LOG.debug("Truncating '" + strTable + "' ...");
        TPCCWorker.TXN_CLIENT.executeStatement("TRUNCATE " + strTable);
    }

    protected int loadWhse(int whseKount) {
        LOG.debug("\nStart Whse Load for " + whseKount + " Whses...");

        Warehouse warehouse = new Warehouse();
        for (int i = 1; i <= whseKount; i++) {
            warehouse.w_id = i;
            warehouse.w_ytd = 300000;

            // random within [0.0000 .. 0.2000]
            warehouse.w_tax = (double) ((TPCCUtil.randomNumber(0, 2000, gen)) / 10000.0);

            warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, gen));
            warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
            warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
            warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
            warehouse.w_state = TPCCUtil.randomStr(3).toUpperCase();
            warehouse.w_zip = "123456789";

            String statement = "INSERT INTO " + TPCCConstants.TABLENAME_WAREHOUSE + " (W_ID, W_YTD, W_TAX, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP) VALUES (" + warehouse.w_id + ", "
                    + warehouse.w_ytd + ", " + warehouse.w_tax + ", '" + warehouse.w_name + "', '" + warehouse.w_street_1 + "', '" + warehouse.w_street_2 + "', '" + warehouse.w_city + "', '"
                    + warehouse.w_state + "', '" + warehouse.w_zip + "')";
            TPCCWorker.TXN_CLIENT.executeStatement(statement);
        }

        LOG.debug("Done Whse Load");
        return whseKount;
    }

    protected int loadItem(int itemKount) {
        int k = 0;
        int t = 0;
        int randPct = 0;
        int len = 0;
        int startORIGINAL = 0;
        t = itemKount;
        LOG.debug("\nStart Item Load for " + t + " Items...");

        Item item = new Item();

        for (int i = 1; i <= itemKount; i++) {
            item.i_id = i;
            item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24, gen));
            item.i_price = (double) (TPCCUtil.randomNumber(100, 10000, gen) / 100.0);

            // i_data
            randPct = TPCCUtil.randomNumber(1, 100, gen);
            len = TPCCUtil.randomNumber(26, 50, gen);
            if (randPct > 10) {
                // 90% of time i_data isa random string of length [26 .. 50]
                item.i_data = TPCCUtil.randomStr(len);
            } else {
                // 10% of time i_data has "ORIGINAL" crammed somewhere in
                // middle
                startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), gen);
                item.i_data = TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TPCCUtil.randomStr(len - startORIGINAL - 9);
            }

            item.i_im_id = TPCCUtil.randomNumber(1, 10000, gen);

            k++;

            LOG.debug("Writing record " + k + " of " + t);
            String statement = "INSERT INTO " + TPCCConstants.TABLENAME_ITEM + " (I_ID, I_NAME, I_PRICE, I_DATA, I_IM_ID) VALUES (" + item.i_id + ",'" + item.i_name + "', " + item.i_price + ", '" + item.i_data + "', " + item.i_im_id + ")";
            TPCCWorker.TXN_CLIENT.executeStatement(statement);
        }
        LOG.debug("End Item Load for records " + k
                + " of " + t);
        return k;
    }

    protected int loadStock(int whseKount, int itemKount) {
        int k = 0;
        int t = 0;
        int randPct = 0;
        int len = 0;
        int startORIGINAL = 0;
        t = (whseKount * itemKount);
        LOG.debug("\nStart Stock Load for " + t + " units...");
        Stock stock = new Stock();
        for (int i = 1; i <= itemKount; i++) {
            for (int w = 1; w <= whseKount; w++) {
                stock.s_i_id = i;
                stock.s_w_id = w;
                stock.s_quantity = TPCCUtil.randomNumber(10, 100, gen);
                stock.s_ytd = 0;
                stock.s_order_cnt = 0;
                stock.s_remote_cnt = 0;

                // s_data
                randPct = TPCCUtil.randomNumber(1, 100, gen);
                len = TPCCUtil.randomNumber(26, 50, gen);
                if (randPct > 10) {
                    // 90% of time i_data isa random string of length [26 ..
                    // 50]
                    stock.s_data = TPCCUtil.randomStr(len);
                } else {
                    // 10% of time i_data has "ORIGINAL" crammed somewhere
                    // in middle
                    startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), gen);
                    stock.s_data = TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TPCCUtil.randomStr(len - startORIGINAL - 9);
                }

                stock.s_dist_01 = TPCCUtil.randomStr(24);
                stock.s_dist_02 = TPCCUtil.randomStr(24);
                stock.s_dist_03 = TPCCUtil.randomStr(24);
                stock.s_dist_04 = TPCCUtil.randomStr(24);
                stock.s_dist_05 = TPCCUtil.randomStr(24);
                stock.s_dist_06 = TPCCUtil.randomStr(24);
                stock.s_dist_07 = TPCCUtil.randomStr(24);
                stock.s_dist_08 = TPCCUtil.randomStr(24);
                stock.s_dist_09 = TPCCUtil.randomStr(24);
                stock.s_dist_10 = TPCCUtil.randomStr(24);

                k++;

                String statement = "INSERT INTO " + TPCCConstants.TABLENAME_STOCK + " VALUES (" + stock.s_w_id + "," + stock.s_i_id + ", " + stock.s_quantity + ", " + stock.s_ytd + ", "
                        + stock.s_order_cnt + ", " + stock.s_remote_cnt + ", " + stock.s_data + ", " + stock.s_dist_01 + ", " + stock.s_dist_02 + ", " + stock.s_dist_03 + ", " + stock.s_dist_04
                        + ", " + stock.s_dist_05 + ", " + stock.s_dist_06 + ", " + stock.s_dist_07 + ", " + stock.s_dist_08 + ", " + stock.s_dist_09 + ", " + stock.s_dist_10 + ")";
                TPCCWorker.TXN_CLIENT.executeStatement(statement);
                LOG.debug("Writing record " + k + " of " + t);
            }
        }

        LOG.debug("End Stock Load");
        return k;
    }

    protected int loadDist(int whseKount, int distWhseKount) {
        int k = 0;
        int t = 0;

        District district = new District();
        t = (whseKount * distWhseKount);
        LOG.debug("\nStart District Data for " + t + " Dists ...");
        for (int w = 1; w <= whseKount; w++) {
            for (int d = 1; d <= distWhseKount; d++) {
                district.d_id = d;
                district.d_w_id = w;
                district.d_ytd = 30000;

                // random within [0.0000 .. 0.2000]
                district.d_tax = (float) ((TPCCUtil.randomNumber(0, 2000, gen)) / 10000.0);

                district.d_next_o_id = 3001;
                district.d_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, gen));
                district.d_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                district.d_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                district.d_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                district.d_state = TPCCUtil.randomStr(3).toUpperCase();
                district.d_zip = "123456789";

                k++;

                String statement = "INSERT INTO " + TPCCConstants.TABLENAME_DISTRICT + " VALUES (" + district.d_w_id + "," + district.d_id + ", " + district.d_ytd + ", " + district.d_tax + ", "
                        + district.d_next_o_id + ", " + district.d_name + ", " + district.d_street_1 + ", " + district.d_street_2 + ", " + district.d_city + ", " + district.d_state + ", "
                        + district.d_zip + ")";
                TPCCWorker.TXN_CLIENT.executeStatement(statement);
                LOG.debug("Writing record " + k + " of " + t);
            }
        }

        LOG.debug("End District Load");
        return k;
    }

    protected int loadCust(int whseKount, int distWhseKount, int custDistKount) {
        int k = 0;
        int t = 0;

        Customer customer = new Customer();
        History history = new History();
        t = (whseKount * distWhseKount * custDistKount * 2);
        LOG.debug("\nStart Cust-Hist Load for " + t + " Cust-Hists...");

        for (int w = 1; w <= whseKount; w++) {
            for (int d = 1; d <= distWhseKount; d++) {
                for (int c = 1; c <= custDistKount; c++) {
                    Timestamp sysdate = new java.sql.Timestamp(System.currentTimeMillis());

                    customer.c_id = c;
                    customer.c_d_id = d;
                    customer.c_w_id = w;

                    // discount is random between [0.0000 ... 0.5000]
                    customer.c_discount = (float) (TPCCUtil.randomNumber(1, 5000, gen) / 10000.0);

                    if (TPCCUtil.randomNumber(1, 100, gen) <= 10) {
                        customer.c_credit = "BC"; // 10% Bad Credit
                    } else {
                        customer.c_credit = "GC"; // 90% Good Credit
                    }
                    if (c <= 1000) {
                        customer.c_last = TPCCUtil.getLastName(c - 1);
                    } else {
                        customer.c_last = TPCCUtil.getNonUniformRandomLastNameForLoad(gen);
                    }
                    customer.c_first = TPCCUtil.randomStr(TPCCUtil.randomNumber(8, 16, gen));
                    customer.c_credit_lim = 50000;

                    customer.c_balance = -10;
                    customer.c_ytd_payment = 10;
                    customer.c_payment_cnt = 1;
                    customer.c_delivery_cnt = 0;

                    customer.c_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                    customer.c_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                    customer.c_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                    customer.c_state = TPCCUtil.randomStr(3).toUpperCase();
                    // TPC-C 4.3.2.7: 4 random digits + "11111"
                    customer.c_zip = TPCCUtil.randomNStr(4) + "11111";

                    customer.c_phone = TPCCUtil.randomNStr(16);

                    customer.c_since = sysdate;
                    customer.c_middle = "OE";
                    customer.c_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(300, 500, gen));

                    history.h_c_id = c;
                    history.h_c_d_id = d;
                    history.h_c_w_id = w;
                    history.h_d_id = d;
                    history.h_w_id = w;
                    history.h_date = sysdate;
                    history.h_amount = 10;
                    history.h_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 24, gen));

                    k = k + 2;

                    String custStatement = "INSERT INTO " + TPCCConstants.TABLENAME_CUSTOMER + " VALUES (" + customer.c_w_id + "," + customer.c_d_id + ", " + customer.c_id + ", "
                            + customer.c_discount + ", " + customer.c_credit + ", " + customer.c_last + ", " + customer.c_first + ", " + customer.c_credit_lim + ", " + customer.c_balance + ", "
                            + customer.c_ytd_payment + ", " + customer.c_payment_cnt + ", " + customer.c_delivery_cnt + ", " + customer.c_street_1 + ", " + customer.c_street_2 + ", "
                            + customer.c_city + ", " + customer.c_state + ", " + customer.c_zip + ", " + customer.c_phone + ")";
                    TPCCWorker.TXN_CLIENT.executeStatement(custStatement);

                    String historyStatement = "INSERT INTO " + TPCCConstants.TABLENAME_HISTORY + " VALUES (" + history.h_c_id + "," + history.h_c_d_id + ", " + history.h_c_w_id + ", "
                            + history.h_d_id + ", " + history.h_w_id + ", " + history.h_date + ", " + history.h_amount + ", " + history.h_data + ")";
                    TPCCWorker.TXN_CLIENT.executeStatement(historyStatement);
                }
            }
        }

        LOG.debug("End Cust-Hist Data Load");

        return k;
    }

    protected int loadOrder(int whseKount, int distWhseKount, int custDistKount) {
        int k = 0;
        int t = 0;
        Oorder oorder = new Oorder();
        NewOrder new_order = new NewOrder();
        OrderLine order_line = new OrderLine();

        t = (whseKount * distWhseKount * custDistKount);
        t = (t * 11) + (t / 3);
        LOG.debug("whse=" + whseKount + ", dist=" + distWhseKount + ", cust=" + custDistKount);
        LOG.debug("\nStart Order-Line-New Load for approx " + t + " rows...");

        for (int w = 1; w <= whseKount; w++) {

            for (int d = 1; d <= distWhseKount; d++) {
                // TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
                int[] c_ids = new int[custDistKount];
                for (int i = 0; i < custDistKount; ++i) {
                    c_ids[i] = i + 1;
                }
                // Collections.shuffle exists, but there is no
                // Arrays.shuffle
                for (int i = 0; i < c_ids.length - 1; ++i) {
                    int remaining = c_ids.length - i - 1;
                    int swapIndex = gen.nextInt(remaining) + i + 1;
                    assert i < swapIndex;
                    int temp = c_ids[swapIndex];
                    c_ids[swapIndex] = c_ids[i];
                    c_ids[i] = temp;
                }

                for (int c = 1; c <= custDistKount; c++) {

                    oorder.o_id = c;
                    oorder.o_w_id = w;
                    oorder.o_d_id = d;
                    oorder.o_c_id = c_ids[c - 1];
                    // o_carrier_id is set *only* for orders with ids < 2101
                    // [4.3.3.1]
                    if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
                        oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10, gen);
                    } else {
                        oorder.o_carrier_id = null;
                    }
                    oorder.o_ol_cnt = TPCCUtil.randomNumber(5, 15, gen);
                    oorder.o_all_local = 1;
                    oorder.o_entry_d = System.currentTimeMillis();

                    k++;

                    String openOrderStatement = "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER + " VALUES (" + oorder.o_w_id + "," + oorder.o_d_id + ", " + oorder.o_id + ", " + oorder.o_c_id
                            + ", " + oorder.o_carrier_id + ", " + oorder.o_ol_cnt + ", " + new java.sql.Timestamp(oorder.o_entry_d).getTime() + ")";
                    TPCCWorker.TXN_CLIENT.executeStatement(openOrderStatement);

                    // 900 rows in the NEW-ORDER table corresponding to the
                    // last
                    // 900 rows in the ORDER table for that district (i.e.,
                    // with
                    // NO_O_ID between 2,101 and 3,000)

                    if (c >= FIRST_UNPROCESSED_O_ID) {

                        new_order.no_w_id = w;
                        new_order.no_d_id = d;
                        new_order.no_o_id = c;

                        k++;

                        String newOrderStatement = "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER + " VALUES (" + new_order.no_w_id + "," + new_order.no_d_id + ", " + new_order.no_o_id + ")";
                        TPCCWorker.TXN_CLIENT.executeStatement(newOrderStatement);
                    } // end new order

                    for (int l = 1; l <= oorder.o_ol_cnt; l++) {
                        order_line.ol_w_id = w;
                        order_line.ol_d_id = d;
                        order_line.ol_o_id = c;
                        order_line.ol_number = l; // ol_number
                        order_line.ol_i_id = TPCCUtil.randomNumber(1, 100000, gen);
                        if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
                            order_line.ol_delivery_d = oorder.o_entry_d;
                            order_line.ol_amount = 0;
                        } else {
                            order_line.ol_delivery_d = null;
                            // random within [0.01 .. 9,999.99]
                            order_line.ol_amount = (float) (TPCCUtil.randomNumber(1, 999999, gen) / 100.0);
                        }

                        order_line.ol_supply_w_id = order_line.ol_w_id;
                        order_line.ol_quantity = 5;
                        order_line.ol_dist_info = TPCCUtil.randomStr(24);

                        k++;

                        String orderLineStatement = "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE + " VALUES (" + order_line.ol_w_id + "," + order_line.ol_d_id + ", " + order_line.ol_o_id + ", "
                                + order_line.ol_number + ", " + order_line.ol_i_id + ", " + order_line.ol_delivery_d == null ? null : new Timestamp(order_line.ol_delivery_d).getTime() + ", "
                                + order_line.ol_amount + ", " + order_line.ol_supply_w_id + ", " + order_line.ol_quantity + ", " + order_line.ol_dist_info + ")";
                        TPCCWorker.TXN_CLIENT.executeStatement(orderLineStatement);
                    }
                }
            }
        }

        LOG.debug("  Writing final records " + k + " of " + t);
        LOG.debug("End Orders Load");

        return k;
    }

}
