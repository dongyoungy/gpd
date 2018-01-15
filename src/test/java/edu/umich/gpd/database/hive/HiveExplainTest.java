package edu.umich.gpd.database.hive;

import edu.umich.gpd.userinput.DatabaseInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Dong Young Yoon on 1/15/18.
 */
public class HiveExplainTest {
  public static void main(String[] args) {
    DatabaseInfo dbInfo = new DatabaseInfo();

    if (args.length != 3) {
      System.out.println("USAGE: HiveExplainTest <host> <port> <dbname>");
      return;
    }

    dbInfo.setHost(args[0]);
    dbInfo.setPort(Integer.parseInt(args[1]));
    String dbName = args[2];

    HiveJDBCConnection hiveConnection = new HiveJDBCConnection();

    Connection conn = hiveConnection.getConnection(dbInfo);

    try {
      conn.setCatalog(dbName);
      Statement stmt = conn.createStatement();
      stmt.execute("USE " + dbName);
      ResultSet res = stmt.executeQuery("EXPLAIN select\n" +
          "\tc_custkey,\n" +
          "\tc_name,\n" +
          "\tsum(l_extendedprice * (1 - l_discount)) as revenue,\n" +
          "\tc_acctbal,\n" +
          "\tn_name,\n" +
          "\tc_address,\n" +
          "\tc_phone,\n" +
          "\tc_comment\n" +
          "from\n" +
          "\tcustomer,\n" +
          "\torders,\n" +
          "\tlineitem,\n" +
          "\tnation\n" +
          "where\n" +
          "\tc_custkey = o_custkey\n" +
          "\tand l_orderkey = o_orderkey\n" +
          "\tand o_orderdate >= date '1993-08-01'\n" +
          "\tand o_orderdate < date '1993-08-01' + interval '3' month\n" +
          "\tand l_returnflag = 'R'\n" +
          "\tand c_nationkey = n_nationkey\n" +
          "group by\n" +
          "\tc_custkey,\n" +
          "\tc_name,\n" +
          "\tc_acctbal,\n" +
          "\tc_phone,\n" +
          "\tn_name,\n" +
          "\tc_address,\n" +
          "\tc_comment\n" +
          "order by\n" +
          "\trevenue desc\n" +
          "limit 20");
      while (res.next()) {
        System.out.println(res.getString(1));
      }
      res.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
