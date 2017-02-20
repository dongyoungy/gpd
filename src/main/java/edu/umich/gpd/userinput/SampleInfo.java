package edu.umich.gpd.userinput;

/**
 * Created by Dong Young Yoon on 2/19/17.
 */
public class SampleInfo {
  private String dbName;
  private double ratio;

  public SampleInfo(String dbName, double ratio) {
    this.dbName = dbName;
    this.ratio = ratio;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public double getRatio() {
    return ratio;
  }

  public void setRatio(double ratio) {
    this.ratio = ratio;
  }
}
