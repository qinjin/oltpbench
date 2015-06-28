package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
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
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

public class OrderStatusExt extends MDTCProcedure {
    private static final Logger LOG = Logger.getLogger(OrderStatusExt.class);

    public static final String OS_CUSTOMER_BY_NAME = "OS_CUSTOMER_BY_NAME";
    public static final String OS_GET_CUST = "OS_GET_CUST";
    public static final String OS_GET_ORDER_LINES = "OS_GET_ORDER_LINES";
    public static final String OS_GET_NEW_EST_ORDER = "OS_GET_NEW_EST_ORDER";

    private final String STMT_GET_NEW_EST_ORDER = "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM " + TPCCConstants.TABLENAME_OPENORDER + " WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ?";
    private final String STMT_GET_ORDER_LINES = "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY," + " OL_AMOUNT, OL_DELIVERY_D" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + " WHERE OL_O_ID = ?"
            + " AND OL_D_ID =?" + " AND OL_W_ID = ?";
    private final String STMT_GET_CUST = "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " + "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
            + "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE " + "C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";
    private final String STMT_CUSTOMER_BY_NAME = "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " + "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, "
            + "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ?";

    public void run(TransactionClient txnClient, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        // initializing all prepared statements
        initStatements(txnClient);

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        boolean isCustomerByName = false;
        int y = TPCCUtil.randomNumber(1, 100, gen);
        String customerLastName = null;
        int customerID = -1;
        if (y <= 60) {
            isCustomerByName = true;
            customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
        } else {
            isCustomerByName = false;
            customerID = TPCCUtil.getCustomerID(gen);
        }

        orderStatusTransaction(terminalWarehouseID, districtID, customerID, customerLastName, isCustomerByName, txnClient, w);
    }

    public void initStatements(TransactionClient txnClient) {
        txnClient.setPrepareStatement(OS_GET_NEW_EST_ORDER, STMT_GET_NEW_EST_ORDER);
        txnClient.setPrepareStatement(OS_GET_ORDER_LINES, STMT_GET_ORDER_LINES);
        txnClient.setPrepareStatement(OS_GET_CUST, STMT_GET_CUST);
        txnClient.setPrepareStatement(OS_CUSTOMER_BY_NAME, STMT_CUSTOMER_BY_NAME);
    }

    // attention duplicated code across trans... ok for now to maintain separate
    // prepared statements
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, TransactionClient txnClient) {
        ResultSet rs;
        Row resultRow;

        // statement = new BoundStatement(payGetCust).bind(1, c_w_id).bind(2,
        // c_d_id).bind(3, c_id);
        rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(OS_GET_CUST, String.valueOf(c_w_id), c_w_id, c_d_id, c_id));
        if (!rs.iterator().hasNext()) {
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        resultRow = rs.iterator().next();
        Customer c = MDTCUtil.newCustomerFromResults(resultRow);
        c.c_id = c_id;
        c.c_last = resultRow.getString("C_LAST");
        return c;
    }

    private void orderStatusTransaction(int w_id, int d_id, int c_id, String c_last, boolean c_by_name, TransactionClient txnClient, TPCCWorker w) {
        int o_id = -1, o_carrier_id = -1;
        Timestamp entdate;
        ArrayList<String> orderLines = new ArrayList<String>();

        Customer c;
        if (c_by_name) {
            assert c_id <= 0;
            // TODO: This only needs c_balance, c_first, c_middle, c_id
            // only fetch those columns?
            c = getCustomerByName(w_id, d_id, c_last, txnClient);
        } else {
            assert c_last == null;
            c = getCustomerById(w_id, d_id, c_id, txnClient);
        }

        ResultSet rs;
        Row resultRow;

        // find the newest order for the customer
        // retrieve the carrier & order date for the most recent order.
        // statement = new BoundStatement(ordStatGetNewestOrd).bind(1,
        // w_id).bind(2, d_id).bind(3, c.c_id);
        rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(OS_GET_NEW_EST_ORDER, String.valueOf(w_id), w_id, d_id, c.c_id));
        if (!rs.iterator().hasNext()) {
            throw new RuntimeException("No orders for O_W_ID=" + w_id + " O_D_ID=" + d_id + " O_C_ID=" + c.c_id);
        }

        List<Row> allRows = Lists.newArrayList(rs.allRows());
        //Sort desc
        Collections.sort(allRows, new Comparator<Row>() {

            @Override
            public int compare(Row o1, Row o2) {
                if (o1 == null && o2 == null) {
                    return 0;
                } else if (o1 == null) {
                    return 1;
                } else if (o2 == null) {
                    return -1;
                } else {
                    return o2.getInt("O_ID") - o1.getInt("O_ID");
                }
            }
        });

        resultRow = allRows.get(0);
        o_id = resultRow.getInt("O_ID");
        o_carrier_id = resultRow.getInt("O_CARRIER_ID");
        entdate = new Timestamp(resultRow.getLong("O_ENTRY_D"));
        rs = null;

        // retrieve the order lines for the most recent order
        // statement = new BoundStatement(ordStatGetOrderLines).bind(1,
        // o_id).bind(2, d_id).bind(3, w_id);
        rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(OS_GET_ORDER_LINES, String.valueOf(w_id), o_id, d_id, w_id));

        Iterator<Row> iter = rs.iterator();
        while (iter.hasNext()) {
            resultRow = iter.next();
            StringBuilder orderLine = new StringBuilder();
            orderLine.append("[");
            orderLine.append(resultRow.getLong("OL_SUPPLY_W_ID"));
            orderLine.append(" - ");
            orderLine.append(resultRow.getLong("OL_I_ID"));
            orderLine.append(" - ");
            orderLine.append(resultRow.getLong("OL_QUANTITY"));
            orderLine.append(" - ");
            orderLine.append(TPCCUtil.formattedDouble(resultRow.getDouble("OL_AMOUNT")));
            orderLine.append(" - ");
            if (resultRow.getLong("OL_DELIVERY_D") != 0)
                orderLine.append(resultRow.getLong("OL_DELIVERY_D"));
            else
                orderLine.append("99-99-9999");
            orderLine.append("]");
            orderLines.add(orderLine.toString());
        }
        rs = null;

        // commit the transaction
        // conn.commit();

        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage.append("\n");
        terminalMessage.append("+-------------------------- ORDER-STATUS -------------------------+\n");
        terminalMessage.append(" Date: ");
        terminalMessage.append(TPCCUtil.getCurrentTime());
        terminalMessage.append("\n\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n District:  ");
        terminalMessage.append(d_id);
        terminalMessage.append("\n\n Customer:  ");
        terminalMessage.append(c.c_id);
        terminalMessage.append("\n   Name:    ");
        terminalMessage.append(c.c_first);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_middle);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_last);
        terminalMessage.append("\n   Balance: ");
        terminalMessage.append(c.c_balance);
        terminalMessage.append("\n\n");
        if (o_id == -1) {
            terminalMessage.append(" Customer has no orders placed.\n");
        } else {
            terminalMessage.append(" Order-Number: ");
            terminalMessage.append(o_id);
            terminalMessage.append("\n    Entry-Date: ");
            terminalMessage.append(entdate);
            terminalMessage.append("\n    Carrier-Number: ");
            terminalMessage.append(o_carrier_id);
            terminalMessage.append("\n\n");
            if (orderLines.size() != 0) {
                terminalMessage.append(" [Supply_W - Item_ID - Qty - Amount - Delivery-Date]\n");
                for (String orderLine : orderLines) {
                    terminalMessage.append(" ");
                    terminalMessage.append(orderLine);
                    terminalMessage.append("\n");
                }
            } else {
                if (LOG.isTraceEnabled())
                    LOG.trace(" This Order has no Order-Lines.\n");
            }
        }
        terminalMessage.append("+-----------------------------------------------------------------+\n\n");
        if (LOG.isTraceEnabled())
            LOG.trace(terminalMessage.toString());
    }

    // attention this code is repeated in other transacitons... ok for now to
    // allow for separate statements.
    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last, TransactionClient txnClient) {
        ArrayList<Customer> customers = new ArrayList<Customer>();
        ResultSet rs;

        // statement = new BoundStatement(customerByName).bind(1,
        // c_w_id).bind(2, c_d_id).bind(3, c_last);
        rs = txnClient.executePreparedStatement(MDTCUtil.buildPreparedStatement(OS_CUSTOMER_BY_NAME, String.valueOf(c_w_id), c_w_id, c_d_id, c_last));
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
