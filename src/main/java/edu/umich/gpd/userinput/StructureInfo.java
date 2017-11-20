package edu.umich.gpd.userinput;

/**
 * Created by Dong Young Yoon on 11/19/17.
 */
public class StructureInfo {
  private String type;
  private String tableName;
  private String columnName;

  public StructureInfo(String type, String tableName, String columnName) {
    this.type = type;
    this.tableName = tableName;
    this.columnName = columnName;
  }

  public String getType() {
    return type;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }
}
