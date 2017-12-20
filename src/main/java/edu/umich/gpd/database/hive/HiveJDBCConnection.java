package edu.umich.gpd.database.hive;

import edu.umich.gpd.database.common.JDBCConnection;
import edu.umich.gpd.userinput.DatabaseInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Dong Young Yoon on 12/20/17.
 */
public class HiveJDBCConnection extends JDBCConnection{

  private static final String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
  @Override
  public Connection getConnection(DatabaseInfo dbInfo) {
    String host = dbInfo.getHost();
    int port = dbInfo.getPort();

    try {
      Class.forName(driverName);
      Connection conn = DriverManager.getConnection(String.format("jdbc:hive2://%s:%d", host, port),
          dbInfo.getId(), dbInfo.getPassword());
      return conn;
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }
}
