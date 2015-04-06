package com.oltpbenchmark.benchmarks.tpcc.mdtc.procedures;

import java.sql.Timestamp;

import mdtc.api.transaction.client.Row;

import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

public class MDTCUtil {

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

}
