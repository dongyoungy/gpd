package edu.umich.gpd.database.hive;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by Dong Young Yoon on 1/18/18.
 */
public abstract class HiveParameter<T> {

  protected Type type;
  protected String key;
  protected T value;
  protected T[] range;

  public HiveParameter(Type type, String key, T value, T[] range) {
    this.type = type;
    this.key = key;
    this.value = value;
    this.range = Arrays.copyOf(range, range.length);
    Arrays.sort(this.range);
  }

  public abstract String getCode();

  public abstract String toString();

  public abstract void apply(Statement stmt) throws SQLException;

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public T changeValueRandom() {
    Random rng = new Random();
    int currentIndex = Arrays.asList(range).indexOf(value);

    int newIndex = rng.nextInt(range.length);
    while (newIndex == currentIndex) {
      newIndex = rng.nextInt(range.length);
    }
    value = range[newIndex];

    return value;
  }

  public T setMaxValue() {
    value = range[range.length-1];
    return value;
  }

  public T setMinValue() {
    value = range[0];
    return value;
  }

  public enum Type {
    BOOLEAN,
    NUMERIC
  };

}
