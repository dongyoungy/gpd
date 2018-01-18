package edu.umich.gpd.database.hive;

/**
 * Created by Dong Young Yoon on 1/18/18.
 */
public class HiveBooleanParameter extends HiveParameter<Boolean> {

  private static Boolean[] BOOLEAN_RANGE = {false, true};

  public HiveBooleanParameter(String key, boolean value) {
    super(Type.BOOLEAN, key, value, BOOLEAN_RANGE);
  }
}
