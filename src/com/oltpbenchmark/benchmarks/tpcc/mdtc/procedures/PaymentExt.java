package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import mdtc.api.transaction.client.ResultSet;
import mdtc.api.transaction.client.Row;
import mdtc.api.transaction.client.TransactionClient;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

public class PaymentExt extends MDTCProcedure {

    private static final Logger LOG = Logger.getLogger(PaymentExt.class);

    private static final String PAY_CUSTOMER_BY_NAME = "PAY_CUSTOMER_BY_NAME";
    private static final String PAY_INSERT_HIST = "PAY_INSERT_HIST";
    private static final String PAY_UPDATE_BAL = "PAY_UPDATE_BAL";
    private static final String PAY_UPDATE_CUST_BALC = "PAY_UPDATE_CUST_BALC";
    private static final String PAY_GET_CUST_C_DATA = "PAY_GET_CUST_C_DATA";
    private static final String PAY_GET_CUST = "PAY_GET_CUST";
    private static final String PAY_GET_DIST = "PAY_GET_DIST";
    private static final String PAY_UPDATE_DIST = "PAY_UPDATE_DIST";
    private static final String PAY_GET_WHSE = "PAY_GET_WHSE";
    private static final String PAY_UPDATE_WHSE = "PAY_UPDATE_WHSE";
    private static final String PAY_GET_YTD = "PAY_GET_YTD";
    private static final String PAY_GET_WYTD = "PAY_GET_WYTD";

    private final String STMT_UPDATE_WHSE = "UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE + " SET W_YTD = ?  WHERE W_ID = ? ";
    private final String STMT_GET_WHSE = "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME" + " FROM " + TPCCConstants.TABLENAME_WAREHOUSE + " WHERE W_ID = ?";
    private final String STMT_UPDATE_DIST = "UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_YTD = ? WHERE D_W_ID = ? AND D_ID = ?";
    private final String STMT_GET_DIST = "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME" + " FROM " + TPCCConstants.TABLENAME_DISTRICT + " WHERE D_W_ID = ? AND D_ID = ?";
    private final String STMT_GET_CUST = "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " + "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
            + "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE " + "C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";
    private final String STMT_GET_CUST_C_DATA = "SELECT C_DATA FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";
    private final String STMT_UPDATE_CUST_BALC = "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = ?, C_YTD_PAYMENT = ?, " + "C_PAYMENT_CNT = ?, C_DATA = ? "
            + "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";
    private final String STMT_UPDATE_BAL = "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = ?, C_YTD_PAYMENT = ?, " + "C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";
    private final String STMT_INSERT_HIST = "INSERT INTO " + TPCCConstants.TABLENAME_HISTORY + " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " + " VALUES (?,?,?,?,?,?,?,?)";
    private final String STMT_CUSTOMER_BY_NAME = "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " + "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, "
            + "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " " + "WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ?";
    private final String STMT_GET_YTD = "SELECT D_YTD FROM " + TPCCConstants.TABLENAME_DISTRICT + " WHERE D_W_ID = ? AND D_ID = ?";
    private final String STMT_GET_WYTD = "SELECT W_YTD FROM " + TPCCConstants.TABLENAME_WAREHOUSE + " WHERE W_ID = ?";
    
    public void run(TransactionClient txnClient, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        // initializing all prepared statements
        initStatements(txnClient);

        // payUpdateWhse =this.getPreparedStatement(conn, payUpdateWhseSQL);

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = TPCCUtil.getCustomerID(gen);

        int x = TPCCUtil.randomNumber(1, 100, gen);
        int customerDistrictID;
        int customerWarehouseID;
        if (x <= 85) {
            customerDistrictID = districtID;
            customerWarehouseID = terminalWarehouseID;
        } else {
            customerDistrictID = TPCCUtil.randomNumber(1, jTPCCConfig.configDistPerWhse, gen);
            do {
                customerWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
            } while (customerWarehouseID == terminalWarehouseID && numWarehouses > 1);
        }

        long y = TPCCUtil.randomNumber(1, 100, gen);
        boolean customerByName;
        String customerLastName = null;
        customerID = -1;
        if (y <= 60) {
            // 60% lookups by last name
            customerByName = true;
            customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
        } else {
            // 40% lookups by customer ID
            customerByName = false;
            customerID = TPCCUtil.getCustomerID(gen);
        }

        float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);

        paymentTransaction(terminalWarehouseID, customerWarehouseID, paymentAmount, districtID, customerDistrictID, customerID, customerLastName, customerByName, txnClient, w);
    }

    public void initStatements(TransactionClient txnClient) {
        txnClient.setPrepareStatement(PAY_UPDATE_WHSE, STMT_UPDATE_WHSE);
        txnClient.setPrepareStatement(PAY_GET_WHSE, STMT_GET_WHSE);
        txnClient.setPrepareStatement(PAY_UPDATE_DIST, STMT_UPDATE_DIST);
        txnClient.setPrepareStatement(PAY_GET_DIST, STMT_GET_DIST);
        txnClient.setPrepareStatement(PAY_GET_CUST, STMT_GET_CUST);
        txnClient.setPrepareStatement(PAY_GET_CUST_C_DATA, STMT_GET_CUST_C_DATA);
        txnClient.setPrepareStatement(PAY_UPDATE_CUST_BALC, STMT_UPDATE_CUST_BALC);
        txnClient.setPrepareStatement(PAY_UPDATE_BAL, STMT_UPDATE_BAL);
        txnClient.setPrepareStatement(PAY_INSERT_HIST, STMT_INSERT_HIST);
        txnClient.setPrepareStatement(PAY_CUSTOMER_BY_NAME, STMT_CUSTOMER_BY_NAME);
        txnClient.setPrepareStatement(PAY_GET_YTD, STMT_GET_YTD);
        txnClient.setPrepareStatement(PAY_GET_WYTD, STMT_GET_WYTD);
    }

    private void paymentTransaction(int w_id, int c_w_id, float h_amount, int d_id, int c_d_id, int c_id, String c_last, boolean c_by_name, TransactionClient txnClient, TPCCWorker w) {
        String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
        String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;

        ResultSet rs;
        Row resultRow;
        
        //Read before write.
        float w_ytd;
        rs = txnClient.executePreparedStatement(PAY_GET_WYTD, w_id);
        if (!rs.iterator().hasNext())
            throw new RuntimeException("W_ID=" + w_id + " not found!");
        resultRow = rs.iterator().next();
        w_ytd = resultRow.getFloat("W_YTD");
        
        // statement = new BoundStatement(payUpdateWhse).bind(1,
        // h_amount).bind(2, w_id);
        rs = txnClient.executePreparedStatement(PAY_UPDATE_WHSE, h_amount+w_ytd, w_id);
        if (!rs.iterator().hasNext())
            throw new RuntimeException("W_ID=" + w_id + " not found!");

        // statement = new BoundStatement(payGetWhse).bind(1, w_id);
        rs = txnClient.executePreparedStatement(PAY_GET_WHSE, w_id);
        if (!rs.iterator().hasNext())
            throw new RuntimeException("W_ID=" + w_id + " not found!");
        resultRow = rs.iterator().next();
        w_street_1 = resultRow.getString("W_STREET_1");
        w_street_2 = resultRow.getString("W_STREET_2");
        w_city = resultRow.getString("W_CITY");
        w_state = resultRow.getString("W_STATE");
        w_zip = resultRow.getString("W_ZIP");
        w_name = resultRow.getString("W_NAME");
        rs = null;

        int d_ytd;
        //Read before write
        rs = txnClient.executePreparedStatement(PAY_GET_YTD, w_id, d_id);
        if (!rs.iterator().hasNext())
            throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
        resultRow = rs.iterator().next();
        d_ytd = resultRow.getInt("D_YTD");
        
        // statement = new BoundStatement(payUpdateDist).bind(1,
        // h_amount).bind(2, w_id).bind(3, d_id);
        rs = txnClient.executePreparedStatement(PAY_UPDATE_DIST, h_amount+d_ytd, w_id, d_id);
        if (!rs.iterator().hasNext())
            throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");

        // statement = new BoundStatement(payGetDist).bind(1, w_id).bind(2,
        // d_id);
        rs = txnClient.executePreparedStatement(PAY_GET_DIST, w_id, d_id);
        if (!rs.iterator().hasNext())
            throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
        resultRow = rs.iterator().next();
        d_street_1 = resultRow.getString("D_STREET_1");
        d_street_2 = resultRow.getString("D_STREET_2");
        d_city = resultRow.getString("D_CITY");
        d_state = resultRow.getString("D_STATE");
        d_zip = resultRow.getString("D_ZIP");
        d_name = resultRow.getString("D_NAME");
        rs = null;

        Customer c;
        if (c_by_name) {
            assert c_id <= 0;
            c = getCustomerByName(c_w_id, c_d_id, c_last, txnClient);
        } else {
            assert c_last == null;
            c = getCustomerById(c_w_id, c_d_id, c_id, txnClient);
        }

        c.c_balance -= h_amount;
        c.c_ytd_payment += h_amount;
        c.c_payment_cnt += 1;
        String c_data = null;
        if (c.c_credit.equals("BC")) { // bad credit
        // statement = new BoundStatement(payGetCustCdata).bind(1,
        // c_w_id).bind(2, c_d_id).bind(3, c.c_id);
            rs = txnClient.executePreparedStatement(PAY_GET_CUST_C_DATA, c_w_id, c_d_id, c.c_id);
            if (!rs.iterator().hasNext())
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id + " not found!");
            resultRow = rs.iterator().next();
            c_data = resultRow.getString("C_DATA");
            rs = null;

            c_data = c.c_id + " " + c_d_id + " " + c_w_id + " " + d_id + " " + w_id + " " + h_amount + " | " + c_data;
            if (c_data.length() > 500)
                c_data = c_data.substring(0, 500);

            // statement = new BoundStatement(payUpdateCustBalCdata).bind(1,
            // c.c_balance).bind(2, c.c_ytd_payment).bind(3,
            // c.c_payment_cnt).bind(4, c_data).bind(5, c_w_id).bind(6, c_d_id)
            // .bind(7, c.c_id);
            rs = txnClient.executePreparedStatement(PAY_UPDATE_CUST_BALC, c.c_balance, c.c_ytd_payment, c.c_payment_cnt, c_data, c_w_id, c_d_id, c.c_id);
            if (!rs.iterator().hasNext())
                throw new RuntimeException("Error in PYMNT Txn updating Customer C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id);

        } else { // GoodCredit
        // statement = new BoundStatement(payUpdateCustBal).bind(1,
        // c.c_balance).bind(2, c.c_ytd_payment).bind(3,
        // c.c_payment_cnt).bind(4, c_w_id).bind(5, c_d_id).bind(6, c.c_id);
            rs = txnClient.executePreparedStatement(PAY_UPDATE_BAL, c.c_balance, c.c_ytd_payment, c.c_payment_cnt, c_w_id, c_d_id, c.c_id);
            if (!rs.iterator().hasNext())
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id + " not found!");

        }

        if (w_name.length() > 10)
            w_name = w_name.substring(0, 10);
        if (d_name.length() > 10)
            d_name = d_name.substring(0, 10);
        String h_data = w_name + "    " + d_name;

        // statement = new BoundStatement(payInsertHist).bind(1, c_d_id).bind(2,
        // c_w_id).bind(3, c.c_id).bind(4, d_id).bind(5, w_id).bind(6,
        // System.currentTimeMillis()).bind(7, h_amount).bind(8, h_data);
        rs = txnClient.executePreparedStatement(PAY_INSERT_HIST, c_d_id, c_w_id, c.c_id, d_id, w_id, System.currentTimeMillis(), h_amount, h_data);

        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage.append("\n+---------------------------- PAYMENT ----------------------------+");
        terminalMessage.append("\n Date: " + TPCCUtil.getCurrentTime());
        terminalMessage.append("\n\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(w_street_1);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(w_street_2);
        terminalMessage.append("\n   City:    ");
        terminalMessage.append(w_city);
        terminalMessage.append("   State: ");
        terminalMessage.append(w_state);
        terminalMessage.append("  Zip: ");
        terminalMessage.append(w_zip);
        terminalMessage.append("\n\n District:  ");
        terminalMessage.append(d_id);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(d_street_1);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(d_street_2);
        terminalMessage.append("\n   City:    ");
        terminalMessage.append(d_city);
        terminalMessage.append("   State: ");
        terminalMessage.append(d_state);
        terminalMessage.append("  Zip: ");
        terminalMessage.append(d_zip);
        terminalMessage.append("\n\n Customer:  ");
        terminalMessage.append(c.c_id);
        terminalMessage.append("\n   Name:    ");
        terminalMessage.append(c.c_first);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_middle);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_last);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(c.c_street_1);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(c.c_street_2);
        terminalMessage.append("\n   City:    ");
        terminalMessage.append(c.c_city);
        terminalMessage.append("   State: ");
        terminalMessage.append(c.c_state);
        terminalMessage.append("  Zip: ");
        terminalMessage.append(c.c_zip);
        terminalMessage.append("\n   Since:   ");
        if (c.c_since != null) {
            terminalMessage.append(c.c_since.toString());
        } else {
            terminalMessage.append("");
        }
        terminalMessage.append("\n   Credit:  ");
        terminalMessage.append(c.c_credit);
        terminalMessage.append("\n   %Disc:   ");
        terminalMessage.append(c.c_discount);
        terminalMessage.append("\n   Phone:   ");
        terminalMessage.append(c.c_phone);
        terminalMessage.append("\n\n Amount Paid:      ");
        terminalMessage.append(h_amount);
        terminalMessage.append("\n Credit Limit:     ");
        terminalMessage.append(c.c_credit_lim);
        terminalMessage.append("\n New Cust-Balance: ");
        terminalMessage.append(c.c_balance);
        if (c.c_credit.equals("BC")) {
            if (c_data.length() > 50) {
                terminalMessage.append("\n\n Cust-Data: " + c_data.substring(0, 50));
                int data_chunks = c_data.length() > 200 ? 4 : c_data.length() / 50;
                for (int n = 1; n < data_chunks; n++)
                    terminalMessage.append("\n            " + c_data.substring(n * 50, (n + 1) * 50));
            } else {
                terminalMessage.append("\n\n Cust-Data: " + c_data);
            }
        }
        terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");

        if (LOG.isTraceEnabled())
            LOG.trace(terminalMessage.toString());

    }

    // attention duplicated code across trans... ok for now to maintain separate
    // prepared statements
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, TransactionClient txnClient) {

        ResultSet rs;
        Row resultRow;

        // statement = new BoundStatement(payGetCust).bind(1, c_w_id).bind(2,
        // c_d_id).bind(3, c_id);
        rs = txnClient.executePreparedStatement(PAY_GET_CUST, c_w_id, c_d_id, c_id);
        if (!rs.iterator().hasNext()) {
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        resultRow = rs.iterator().next();
        Customer c = MDTCUtil.newCustomerFromResults(resultRow);
        c.c_id = c_id;
        c.c_last = resultRow.getString("C_LAST");
        return c;
    }

    // attention this code is repeated in other transacitons... ok for now to
    // allow for separate statements.
    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last, TransactionClient txnClient) {
        ArrayList<Customer> customers = new ArrayList<Customer>();

        ResultSet rs;
        Row resultRow;

        // statement = new BoundStatement(customerByName).bind(1,
        // c_w_id).bind(2, c_d_id).bind(3, c_last);
        rs = txnClient.executePreparedStatement(PAY_CUSTOMER_BY_NAME, c_w_id, c_d_id, c_last);
        List<Row> allRows = Lists.newArrayList(rs.allRows());
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
                    String c1 = o1.getString("C_FIRST");
                    String c2 = o2.getString("C_FIRST");
                    if (c1 == null && c2 == null) {
                        return 0;
                    } else if (c1 == null) {
                        return -1;
                    } else if (c2 == null) {
                        return 1;
                    } else {
                        return c1.compareTo(c2);
                    }
                }
            }
        });
        
        for (Row row : allRows) {
            Customer c = MDTCUtil.newCustomerFromResults(row);
            c.c_id = row.getInt("C_ID");
            c.c_last = c_last;
            customers.add(c);
        }

        if (customers.size() == 0) {
            throw new RuntimeException("C_LAST=" + c_last + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
        // that
        // counts starting from 1.
        int index = customers.size() / 2;
        if (customers.size() % 2 == 0) {
            index -= 1;
        }
        return customers.get(index);
    }

}
