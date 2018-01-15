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
      ResultSet res = stmt.executeQuery("EXPLAIN SELECT * FROM CUSTOMER");
      while (res.next()) {
        System.out.println(res.getString(1));
      }
      res.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
