package edu.umich.gpd.database;

import edu.umich.gpd.userinput.DatabaseInfo;

import java.sql.Connection;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public abstract class JDBCConnection {

  /**
   * returns a JDBC connection for given database info
   * @param dbInfo database information
   * @return a JDBC connection
   */
  public abstract Connection getConnection(DatabaseInfo dbInfo);
}
