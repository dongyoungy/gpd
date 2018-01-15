package edu.umich.gpd.database.hive;

/**
 * Created by Dong Young Yoon on 1/15/18.
 */
public enum HiveFileType {
  TEXTFILE,
  SEQUENCEFILE,
  AVRO,
  RCFILE,
  PARQUET,
  ORC;

  public String getString() {
    switch (this) {
      case TEXTFILE:
        return "TEXTFILE";
      case SEQUENCEFILE:
        return "SEQUENCEFILE";
      case AVRO:
        return "AVRO";
      case RCFILE:
        return "RCFILE";
      case PARQUET:
        return "PARQUET";
      case ORC:
        return "ORC";
      default:
        return "TEXTFILE";
    }
  }
}
