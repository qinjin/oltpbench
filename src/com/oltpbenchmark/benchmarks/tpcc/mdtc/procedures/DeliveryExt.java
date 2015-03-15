package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.util.Random;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class DeliveryExt extends MDTCProcedure {

    public final String STMT_GET_ORDER_ID = "SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE NO_D_ID = ?" + " AND NO_W_ID = ? ORDER BY NO_O_ID ASC LIMIT 1";
    public final String STMT_DELETE_NEW_ORDER = "SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE NO_D_ID = ?" + " AND NO_W_ID = ? ORDER BY NO_O_ID ASC LIMIT 1";
    public final String STMT_GET_CUST_ID = "SELECT O_C_ID" + " FROM " + TPCCConstants.TABLENAME_OPENORDER + " WHERE O_ID = ?" + " AND O_D_ID = ?" + " AND O_W_ID = ?";
    public final String STMT_UPDATE_CARRIER_ID = "UPDATE " + TPCCConstants.TABLENAME_OPENORDER + " SET O_CARRIER_ID = ?" + " WHERE O_ID = ?" + " AND O_D_ID = ?" + " AND O_W_ID = ?";
    public final String STMT_UPDATE_DELIVERY_DATE = "UPDATE " + TPCCConstants.TABLENAME_ORDERLINE + " SET OL_DELIVERY_D = ?" + " WHERE OL_O_ID = ?" + " AND OL_D_ID = ?" + " AND OL_W_ID = ?";
    public final String STMT_SUM_ORDER_AMOUNT = "SELECT SUM(OL_AMOUNT) AS OL_TOTAL" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + "" + " WHERE OL_O_ID = ?" + " AND OL_D_ID = ?"
            + " AND OL_W_ID = ?";
    public final String STMT_UPDATE_CUST_BAL_DELIVERY_COUNT = "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = C_BALANCE + ?" + ", C_DELIVERY_CNT = C_DELIVERY_CNT + 1"
            + " WHERE C_W_ID = ?" + " AND C_D_ID = ?" + " AND C_ID = ?";

    // Delivery Txn
    private PreparedStatement delivGetOrderId = null;
    private PreparedStatement delivDeleteNewOrder = null;
    private PreparedStatement delivGetCustId = null;
    private PreparedStatement delivUpdateCarrierId = null;
    private PreparedStatement delivUpdateDeliveryDate = null;
    private PreparedStatement delivSumOrderAmount = null;
    private PreparedStatement delivUpdateCustBalDelivCnt = null;

    public Result run(Session session, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        initStatements(session);
        int orderCarrierID = TPCCUtil.randomNumber(1, 10, gen);
        deliveryTransaction(terminalWarehouseID, orderCarrierID, session, w);
        return null;
    }

    public void initStatements(Session session) {
        delivGetOrderId = session.prepare(STMT_GET_ORDER_ID);
        delivDeleteNewOrder = session.prepare(STMT_DELETE_NEW_ORDER);
        delivGetCustId = session.prepare(STMT_GET_CUST_ID);
        delivUpdateCarrierId = session.prepare(STMT_UPDATE_CARRIER_ID);
        delivUpdateDeliveryDate = session.prepare(STMT_UPDATE_DELIVERY_DATE);
        delivSumOrderAmount = session.prepare(STMT_SUM_ORDER_AMOUNT);
        delivUpdateCustBalDelivCnt = session.prepare(STMT_UPDATE_CUST_BAL_DELIVERY_COUNT);
    }

    private int deliveryTransaction(int w_id, int o_carrier_id, Session session, TPCCWorker w) {

        int d_id, c_id;
        float ol_total;
        int[] orderIDs;

        orderIDs = new int[10];
        BoundStatement statement;
        ResultSet rs;
        Row resultRow;

        for (d_id = 1; d_id <= 10; d_id++) {
            statement = new BoundStatement(delivGetOrderId).bind(1, d_id).bind(2, w_id);
            rs = session.execute(statement);
            if (!rs.iterator().hasNext()) {
                // This district has no new orders; this can happen but should
                // be rare
                continue;
            }

            resultRow = rs.iterator().next();
            int no_o_id = resultRow.getInt("NO_O_ID");
            orderIDs[d_id - 1] = no_o_id;
            rs = null;

            statement = new BoundStatement(delivDeleteNewOrder).bind(1, no_o_id).bind(2, d_id).bind(3, w_id);
            rs = session.execute(statement);
            if (rs.all().size() != 1) {
                // This code used to run in a loop in an attempt to make this
                // work
                // with MySQL's default weird consistency level. We just always
                // run
                // this as SERIALIZABLE instead. I don't *think* that fixing
                // this one
                // error makes this work with MySQL's default consistency.
                // Careful
                // auditing would be required.
                throw new UserAbortException("New order w_id=" + w_id + " d_id=" + d_id + " no_o_id=" + no_o_id + " delete failed (not running with SERIALIZABLE isolation?)");
            }

            statement = new BoundStatement(delivGetCustId).bind(1, no_o_id).bind(2, d_id).bind(3, w_id);
            rs = session.execute(statement);
            if (!rs.iterator().hasNext())
                throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");
            resultRow = rs.iterator().next();
            c_id = resultRow.getInt("O_C_ID");
            rs = null;

            statement = new BoundStatement(delivUpdateCarrierId).bind(1, o_carrier_id).bind(2, no_o_id).bind(3, d_id).bind(4, w_id);
            rs = session.execute(statement);
            if (rs.all().size() != 1)
                throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");

            statement = new BoundStatement(delivUpdateDeliveryDate).bind(1, System.currentTimeMillis()).bind(2, no_o_id).bind(3, d_id).bind(4, w_id);
            rs = session.execute(statement);
            if (rs.all().size() == 0)
                throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");

            statement = new BoundStatement(delivSumOrderAmount).bind(1, no_o_id).bind(2, d_id).bind(3, w_id);
            rs = session.execute(statement);
            if (!rs.iterator().hasNext())
                throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");
            resultRow = rs.iterator().next();
            ol_total = resultRow.getFloat("OL_TOTAL");
            rs = null;

            statement = new BoundStatement(delivUpdateCustBalDelivCnt).bind(1, ol_total).bind(2, w_id).bind(3, d_id).bind(4, c_id);
            rs = session.execute(statement);
            if (rs.all().size() == 0)
                throw new RuntimeException("C_ID=" + c_id + " C_W_ID=" + w_id + " C_D_ID=" + d_id + " not found!");
        }

        // TODO: This part is not used
        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage.append("\n+---------------------------- DELIVERY ---------------------------+\n");
        terminalMessage.append(" Date: ");
        terminalMessage.append(TPCCUtil.getCurrentTime());
        terminalMessage.append("\n\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n Carrier:   ");
        terminalMessage.append(o_carrier_id);
        terminalMessage.append("\n\n Delivered Orders\n");
        int skippedDeliveries = 0;
        for (int i = 1; i <= 10; i++) {
            if (orderIDs[i - 1] >= 0) {
                terminalMessage.append("  District ");
                terminalMessage.append(i < 10 ? " " : "");
                terminalMessage.append(i);
                terminalMessage.append(": Order number ");
                terminalMessage.append(orderIDs[i - 1]);
                terminalMessage.append(" was delivered.\n");
            } else {
                terminalMessage.append("  District ");
                terminalMessage.append(i < 10 ? " " : "");
                terminalMessage.append(i);
                terminalMessage.append(": No orders to be delivered.\n");
                skippedDeliveries++;
            }
        }
        terminalMessage.append("+-----------------------------------------------------------------+\n\n");

        return skippedDeliveries;
    }

}
