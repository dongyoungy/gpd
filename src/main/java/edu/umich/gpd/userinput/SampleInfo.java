package edu.umich.gpd.userinput;

import edu.umich.gpd.database.hive.HiveFileType;

/**
 * Created by Dong Young Yoon on 2/19/17.
 */
public class SampleInfo {
  private String dbName;
  private double ratio;

  // Used only for Hive
  private HiveFileType hiveFileType;

  public SampleInfo(String dbName, double ratio) {
    this.dbName = dbName;
    this.ratio = ratio;
  }

  public SampleInfo(String dbName, double ratio, HiveFileType hiveFileType) {
    this.dbName = dbName;
    this.ratio = ratio;
    this.hiveFileType = hiveFileType;
  }

  public HiveFileType getHiveFileType() {
    return hiveFileType;
  }

  public void setHiveFileType(HiveFileType hiveFileType) {
    this.hiveFileType = hiveFileType;
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
