package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures.MDTCProcedure.Result;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpcc.procedures.TPCCProcedure;

public class OrderStatusExt extends MDTCProcedure {

    private static final Logger LOG = Logger.getLogger(OrderStatusExt.class);

    private final String STMT_GET_NEW_EST_ORDER = "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM " + TPCCConstants.TABLENAME_OPENORDER + " WHERE O_W_ID = ?"
            + " AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1";
    private final String STMT_GET_ORDER_LINES = "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY," + " OL_AMOUNT, OL_DELIVERY_D" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + " WHERE OL_O_ID = ?"
            + " AND OL_D_ID =?" + " AND OL_W_ID = ?";
    private final String STMT_GET_CUST = "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " + "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
            + "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE " + "C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";
    private final String STMT_CUSTOMER_BY_NAME = "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " + "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, "
            + "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST";

    private PreparedStatement ordStatGetNewestOrd = null;
    private PreparedStatement ordStatGetOrderLines = null;
    private PreparedStatement payGetCust = null;
    private PreparedStatement customerByName = null;

    public Result run(Session session, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        // initializing all prepared statements
        initStatements(session);

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

        orderStatusTransaction(terminalWarehouseID, districtID, customerID, customerLastName, isCustomerByName, session, w);
        return null;
    }

    public void initStatements(Session session) {
        ordStatGetNewestOrd = session.prepare(STMT_GET_NEW_EST_ORDER);
        ordStatGetOrderLines = session.prepare(STMT_GET_ORDER_LINES);
        payGetCust = session.prepare(STMT_GET_CUST);
        customerByName = session.prepare(STMT_CUSTOMER_BY_NAME);
    }

    // attention duplicated code across trans... ok for now to maintain separate
    // prepared statements
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, Session session) {
        BoundStatement statement;
        ResultSet rs;
        Row resultRow;

        statement = new BoundStatement(payGetCust).bind(1, c_w_id).bind(2, c_d_id).bind(3, c_id);
        rs = session.execute(statement);
        if (!rs.iterator().hasNext()) {
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        resultRow = rs.iterator().next();
        Customer c = MDTCUtil.newCustomerFromResults(resultRow);
        c.c_id = c_id;
        c.c_last = resultRow.getString("C_LAST");
        return c;
    }

    private void orderStatusTransaction(int w_id, int d_id, int c_id, String c_last, boolean c_by_name, Session session, TPCCWorker w) {
        int o_id = -1, o_carrier_id = -1;
        Timestamp entdate;
        ArrayList<String> orderLines = new ArrayList<String>();

        Customer c;
        if (c_by_name) {
            assert c_id <= 0;
            // TODO: This only needs c_balance, c_first, c_middle, c_id
            // only fetch those columns?
            c = getCustomerByName(w_id, d_id, c_last, session);
        } else {
            assert c_last == null;
            c = getCustomerById(w_id, d_id, c_id, session);
        }

        BoundStatement statement;
        ResultSet rs;
        Row resultRow;

        // find the newest order for the customer
        // retrieve the carrier & order date for the most recent order.
        statement = new BoundStatement(ordStatGetNewestOrd).bind(1, w_id).bind(2, d_id).bind(3, c.c_id);
        rs = session.execute(statement);
        if (!rs.iterator().hasNext()) {
            throw new RuntimeException("No orders for O_W_ID=" + w_id + " O_D_ID=" + d_id + " O_C_ID=" + c.c_id);
        }

        resultRow = rs.iterator().next();
        o_id = resultRow.getInt("O_ID");
        o_carrier_id = resultRow.getInt("O_CARRIER_ID");
        entdate = new Timestamp(resultRow.getLong("O_ENTRY_D"));
        rs = null;

        // retrieve the order lines for the most recent order
        statement = new BoundStatement(ordStatGetOrderLines).bind(1, o_id).bind(2, d_id).bind(3, w_id);
        rs = session.execute(statement);

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
    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last, Session session) {
        ArrayList<Customer> customers = new ArrayList<Customer>();
        BoundStatement statement;
        ResultSet rs;
        Row resultRow;

        statement = new BoundStatement(customerByName).bind(1, c_w_id).bind(2, c_d_id).bind(3, c_last);
        rs = session.execute(statement);
        Iterator<Row> iter = rs.iterator();
        while (!iter.hasNext()) {
            resultRow = iter.next();
            Customer c = MDTCUtil.newCustomerFromResults(resultRow);
            c.c_id = resultRow.getInt("C_ID");
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
