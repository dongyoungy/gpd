package edu.umich.gpd.database.hive;

import edu.umich.gpd.util.GPDLogger;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

/**
 * Created by Dong Young Yoon on 1/18/18.
 */
public class HiveNumericParameter extends HiveParameter<Integer> {
  public HiveNumericParameter(String key, Integer value, Integer[] range) {
    super(Type.NUMERIC, key, value, range);
  }

  @Override
  public String getCode() {
    return String.valueOf(Arrays.asList(range).indexOf(value));
  }

  @Override
  public String toString() {
    return String.format("set %s=%d", key, value);
  }

  @Override
  public void apply(Statement stmt) throws SQLException {
    String statement = String.format("set %s=%d", key, value);
    GPDLogger.debug(this, "Executing: " + statement);
    stmt.execute(statement);
  }
}
