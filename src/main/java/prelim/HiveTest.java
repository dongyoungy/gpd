package prelim;

import java.sql.*;

/**
 * Created by Dong Young Yoon on 1/22/17.
 */
public class HiveTest
{
  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  public static void main(String[] args) throws SQLException {
    String workload = args[0];
    String dataSize = args[1];
    String execMem = args[2];
    String applicationName = workload + "_" + dataSize + "_" + execMem;
    String inputData = String.format("/data_gen/HiBench/Data/%s/Input", dataSize);
    String outputData = String.format("/data_gen/HiBench/Data/%s/Output", dataSize);

    try {
      Class.forName(driverName);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }

    Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");

    // temporary test
    Statement stmt = conn.createStatement();
    String tableName = "testHiveDriverTable";
    stmt.execute("drop table if exists " + tableName);
    stmt.execute("create table " + tableName + " (key int, value string)");
    // show tables
    String sql = "show tables '" + tableName + "'";
    System.out.println("Running: " + sql);
    ResultSet res = stmt.executeQuery(sql);
    if (res.next()) {
      System.out.println(res.getString(1));
    }
    // describe table
    sql = "describe " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1) + "\t" + res.getString(2));
    }
  }
}
