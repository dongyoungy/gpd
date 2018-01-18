package edu.umich.gpd.database.hive;

/**
 * Created by Dong Young Yoon on 1/15/18.
 */
public enum HiveFileType {
//  TEXTFILE,
//  SEQUENCEFILE,
//  AVRO,
//  RCFILE,
//  PARQUET,
  ORC;

  public String getString() {
    switch (this) {
//      case TEXTFILE:
//        return "textfile";
//      case SEQUENCEFILE:
//        return "SEQUENCEFILE";
//      case AVRO:
//        return "avro";
//      case RCFILE:
//        return "RCFILE";
//      case PARQUET:
//        return "parquet";
      case ORC:
        return "orc";
      default:
        return "textfile";
    }
  }
}
