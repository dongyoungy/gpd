package edu.umich.gpd.database.mysql;

import edu.umich.gpd.database.JDBCConnection;
import edu.umich.gpd.userinput.DatabaseInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class MySQLJDBCConnection extends JDBCConnection {

  private static final String driverName = "com.mysql.jdbc.Driver";

  @Override
  public Connection getConnection(DatabaseInfo dbInfo) {
    String host = dbInfo.getHost();
    int port = dbInfo.getPort();

    try {
      Class.forName(driverName);
      Connection conn = DriverManager.getConnection(String.format("jdbc:mysql://%s:%d", host, port),
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
