package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.sql.Timestamp;
import java.util.Map;

import mdtc.api.transaction.client.Row;
import mdtc.api.transaction.client.TxnStatement;

import com.google.common.collect.Maps;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

public class MDTCUtil {

    private static final Map<String, String> STATEMENT_TABLE = Maps.newHashMap();

    static {
        //Payment
        STATEMENT_TABLE.put(PaymentExt.PAY_CUSTOMER_BY_NAME, TPCCConstants.TABLENAME_CUSTOMER);
        STATEMENT_TABLE.put(PaymentExt.PAY_INSERT_HIST, TPCCConstants.TABLENAME_HISTORY);
        STATEMENT_TABLE.put(PaymentExt.PAY_UPDATE_BAL, TPCCConstants.TABLENAME_CUSTOMER);
        STATEMENT_TABLE.put(PaymentExt.PAY_UPDATE_CUST_BALC, TPCCConstants.TABLENAME_CUSTOMER);
        STATEMENT_TABLE.put(PaymentExt.PAY_GET_CUST_C_DATA, TPCCConstants.TABLENAME_CUSTOMER);
        STATEMENT_TABLE.put(PaymentExt.PAY_GET_CUST, TPCCConstants.TABLENAME_CUSTOMER);
        STATEMENT_TABLE.put(PaymentExt.PAY_GET_DIST, TPCCConstants.TABLENAME_DISTRICT);
        STATEMENT_TABLE.put(PaymentExt.PAY_UPDATE_DIST, TPCCConstants.TABLENAME_DISTRICT);
        STATEMENT_TABLE.put(PaymentExt.PAY_GET_WHSE, TPCCConstants.TABLENAME_WAREHOUSE);
        STATEMENT_TABLE.put(PaymentExt.PAY_UPDATE_WHSE, TPCCConstants.TABLENAME_WAREHOUSE);
        STATEMENT_TABLE.put(PaymentExt.PAY_GET_YTD, TPCCConstants.TABLENAME_DISTRICT);
        STATEMENT_TABLE.put(PaymentExt.PAY_GET_WYTD, TPCCConstants.TABLENAME_WAREHOUSE);
        //StockLevel
        STATEMENT_TABLE.put(StockLevelExt.SL_GET_COUNT_STOCK, TPCCConstants.TABLENAME_STOCK);
        STATEMENT_TABLE.put(StockLevelExt.SL_GET_DIST_ORDER, TPCCConstants.TABLENAME_DISTRICT);
        //OderStatus
        STATEMENT_TABLE.put(OrderStatusExt.OS_CUSTOMER_BY_NAME, TPCCConstants.TABLENAME_CUSTOMER);
        STATEMENT_TABLE.put(OrderStatusExt.OS_GET_CUST, TPCCConstants.TABLENAME_CUSTOMER);
        STATEMENT_TABLE.put(OrderStatusExt.OS_GET_NEW_EST_ORDER, TPCCConstants.TABLENAME_OPENORDER);
        STATEMENT_TABLE.put(OrderStatusExt.OS_GET_ORDER_LINES, TPCCConstants.TABLENAME_ORDERLINE);
        //Delivery
        STATEMENT_TABLE.put(DeliveryExt.DELIVERY_UPDATE_CUST_BAL_DELIVERY_COUNT, TPCCConstants.TABLENAME_CUSTOMER);
        STATEMENT_TABLE.put(DeliveryExt.DELIVERY_DELETE_NEW_ORDER, TPCCConstants.TABLENAME_NEWORDER);
        STATEMENT_TABLE.put(DeliveryExt.DELIVERY_GET_CUST_BAN, TPCCConstants.TABLENAME_CUSTOMER);
        STATEMENT_TABLE.put(DeliveryExt.DELIVERY_GET_CUST_ID, TPCCConstants.TABLENAME_OPENORDER);
        STATEMENT_TABLE.put(DeliveryExt.DELIVERY_GET_DELIVERY_COUNT, TPCCConstants.TABLENAME_CUSTOMER);
        STATEMENT_TABLE.put(DeliveryExt.DELIVERY_GET_OL_NUMBER, TPCCConstants.TABLENAME_ORDERLINE);
        STATEMENT_TABLE.put(DeliveryExt.DELIVERY_GET_ORDER_ID, TPCCConstants.TABLENAME_NEWORDER);
        STATEMENT_TABLE.put(DeliveryExt.DELIVERY_SUM_ORDER_AMOUNT, TPCCConstants.TABLENAME_ORDERLINE);
        STATEMENT_TABLE.put(DeliveryExt.DELIVERY_UPDATE_CARRIER_ID, TPCCConstants.TABLENAME_OPENORDER);
        STATEMENT_TABLE.put(DeliveryExt.DELIVERY_UPDATE_DELIVERY_DATE, TPCCConstants.TABLENAME_ORDERLINE);
        //NewOrder
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_GET_CUST_CQL, TPCCConstants.TABLENAME_CUSTOMER);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_GET_DIST_CQL, TPCCConstants.TABLENAME_DISTRICT);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_GET_ITEM_CQL, TPCCConstants.TABLENAME_ITEM);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_GET_NEXT_ORDER_ID, TPCCConstants.TABLENAME_DISTRICT);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_GET_ORDER_CNT, TPCCConstants.TABLENAME_STOCK);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_GET_REMOTE_CNT, TPCCConstants.TABLENAME_STOCK);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_GET_STOCK_CQL, TPCCConstants.TABLENAME_STOCK);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_GET_STOCK_YTD, TPCCConstants.TABLENAME_STOCK);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_GET_WH_CQL, TPCCConstants.TABLENAME_WAREHOUSE);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_INSERT_NEW_ORDER_CQL, TPCCConstants.TABLENAME_NEWORDER);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_INSERT_ORDER_CQL, TPCCConstants.TABLENAME_OPENORDER);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_INSERT_ORDER_LINE_CQL, TPCCConstants.TABLENAME_ORDERLINE);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_UPDATE_DIST_CQL, TPCCConstants.TABLENAME_DISTRICT);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_UPDATE_STOCK_CQL, TPCCConstants.TABLENAME_STOCK);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_GET_OPEN_ORDER, TPCCConstants.TABLENAME_OPENORDER);
        STATEMENT_TABLE.put(NewOrderExt.NEWORDER_GET_NEW_ORDER, TPCCConstants.TABLENAME_NEWORDER);
    }

    public static Customer newCustomerFromResults(Row rs) {
        Customer c = new Customer();
        c.c_first = rs.getString("c_first");
        c.c_middle = rs.getString("c_middle");
        c.c_street_1 = rs.getString("c_street_1");
        c.c_street_2 = rs.getString("c_street_2");
        c.c_city = rs.getString("c_city");
        c.c_state = rs.getString("c_state");
        c.c_zip = rs.getString("c_zip");
        c.c_phone = rs.getString("c_phone");
        c.c_credit = rs.getString("c_credit");
        c.c_credit_lim = rs.getFloat("c_credit_lim");
        c.c_discount = rs.getFloat("c_discount");
        c.c_balance = rs.getFloat("c_balance");
        c.c_ytd_payment = rs.getFloat("c_ytd_payment");
        c.c_payment_cnt = rs.getInt("c_payment_cnt");
        c.c_since = new Timestamp(rs.getLong("c_since"));
        return c;
    }

    public static TxnStatement buildPreparedStatement(boolean isRead, String name, String key, Object... parameters) {
        TxnStatement statement = new TxnStatement(name.toLowerCase(), key.toLowerCase(), STATEMENT_TABLE.get(name).toLowerCase(), parameters, isRead);
        return statement;
    }
}
