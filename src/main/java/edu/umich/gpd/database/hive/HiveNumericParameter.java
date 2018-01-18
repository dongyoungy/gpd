package edu.umich.gpd.database.hive;

/**
 * Created by Dong Young Yoon on 1/18/18.
 */
public class HiveNumericParameter extends HiveParameter<Integer> {
  public HiveNumericParameter(String key, Integer value, Integer[] range) {
    super(Type.NUMERIC, key, value, range);
  }
}
