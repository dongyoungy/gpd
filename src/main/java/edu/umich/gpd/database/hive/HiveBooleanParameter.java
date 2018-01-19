package edu.umich.gpd.database.hive;

import edu.umich.gpd.util.GPDLogger;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Dong Young Yoon on 1/18/18.
 */
public class HiveBooleanParameter extends HiveParameter<Boolean> {

  private static Boolean[] BOOLEAN_RANGE = {false, true};

  public HiveBooleanParameter(String key, boolean value) {
    super(Type.BOOLEAN, key, value, BOOLEAN_RANGE);
  }

  @Override
  public String getCode() {
    return (value.booleanValue() ? "T" : "F");
  }

  @Override
  public String toString() {
    return String.format("set %s=%s", key, (value ? "true" : "false"));
  }

  @Override
  public void apply(Statement stmt) throws SQLException {
    String statement = String.format("set %s=%s", key, (value ? "true" : "false"));
    GPDLogger.debug(this, "Executing: " + statement);
    stmt.execute(statement);
  }
}
