package edu.umich.gpd.database.mysql;

import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.schema.Table;
import edu.umich.gpd.util.GPDLogger;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

/** Created by Dong Young Yoon on 2/13/17. */
public class MySQLIndex extends Structure {

  public MySQLIndex(String name, Table table) {
    super(name, table);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = result * prime + table.getName().hashCode();
    for (ColumnDefinition cd : columns) {
      result = result * prime + cd.getColumnName().hashCode();
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof MySQLIndex) {
      MySQLIndex other = (MySQLIndex) o;
      if (!other.getTable().getName().equals(this.table.getName())) {
        return false;
      }
      List<ColumnDefinition> otherColumns = other.getColumns();
      if (columns.size() != otherColumns.size()) {
        return false;
      } else {
        int idx = 0;
        while (idx < columns.size()) {
          if (!columns.get(idx).getColumnName().equals(otherColumns.get(idx).getColumnName())) {
            return false;
          }
          ++idx;
        }
        return true;
      }
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    String columnStr = "";
    int length = columns.size();
    int i = 0;
    for (ColumnDefinition colDef : columns) {
      columnStr += colDef.getColumnName();
      if (i < length - 1) {
        columnStr += ",";
      }
      ++i;
    }
    return String.format("CREATE INDEX %s ON %s (%s);", this.name, table.getName(), columnStr);
  }

  public String getQueryString() {
    String columnStr = "";
    int length = columns.size();
    int i = 0;
    for (ColumnDefinition colDef : columns) {
      columnStr += colDef.getColumnName();
      if (i < length - 1) {
        columnStr += ",";
      }
      ++i;
    }
    return String.format("CREATE INDEX %s ON %s (%s);", this.name, table.getName(), columnStr);
  }

  @Override
  public String getNonUniqueString() {
    String columnStr = "";
    int length = columns.size();
    int i = 0;
    for (ColumnDefinition colDef : columns) {
      columnStr += colDef.getColumnName();
      if (i < length - 1) {
        columnStr += ",";
      }
      ++i;
    }
    return String.format("CREATE INDEX ON %s (%s);", table.getName(), columnStr);
  }

  @Override
  public String getColumnString() {
    if (!columnString.isEmpty()) {
      return columnString;
    }
    String columnStr = "";
    int length = columns.size();
    int i = 0;
    for (ColumnDefinition colDef : columns) {
      columnStr += colDef.getColumnName();
      if (i < length - 1) {
        columnStr += ",";
      }
      ++i;
    }
    columnString = columnStr;
    return columnStr;
  }

  public boolean create(Connection conn, String dbName) {
    String columnStr = "";
    // ColumnDefinition[] cols = (ColumnDefinition[])columns.toArray();
    List<ColumnDefinition> columnList = new ArrayList<>(columns);
    for (int i = 0; i < columnList.size(); ++i) {
      columnStr += columnList.get(i).getColumnName();
      if (i < columnList.size() - 1) {
        columnStr += ",";
      }
    }
    try {
      conn.setCatalog(dbName);
      Statement stmt = conn.createStatement();
      String targetDBName = conn.getCatalog();
      stmt.execute(
          String.format("CREATE INDEX %s ON %s (%s)", this.name, table.getName(), columnStr));
      GPDLogger.debug(
          this,
          "Executed: "
              + String.format(
                  "CREATE INDEX %s ON %s (%s) @ %s",
                  this.name, table.getName(), columnStr, targetDBName));

      ResultSet res =
          stmt.executeQuery(
              String.format(
                  "SELECT stat_value*@@innodb_page_size FROM "
                      + "mysql.innodb_index_stats WHERE stat_name = 'size' and database_name = '%s' and "
                      + "index_name = '%s'",
                  dbName, this.name));
      if (res.next()) {
        this.size = res.getLong(1);
      } else {
        GPDLogger.info(this, "Failed to obtain the size of this physical " + "structure: " + name);
      }
      res.close();
      res =
          stmt.executeQuery(
              String.format("SELECT COUNT(DISTINCT %s) FROM %s;", columnStr, table.getName()));
      if (res.next()) {
        this.table.setDistinctRowCount(dbName, columnStr, res.getLong(1));
      } else {
        GPDLogger.info(
            this,
            "Failed to obtain the distinct row count of this physical " + "structure: " + name);
      }
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean drop(Connection conn, String dbName) {
    try {
      conn.setCatalog(dbName);
      Statement stmt = conn.createStatement();
      String targetDBName = conn.getCatalog();
      stmt.execute(String.format("DROP INDEX %s ON %s", this.name, table.getName()));
      GPDLogger.debug(
          this,
          "Executed: "
              + String.format(
                  "DROP INDEX %s ON %s @ %s", this.name, table.getName(), targetDBName));
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean isCovering(Structure other) {
    if (!(other instanceof MySQLIndex || other instanceof MySQLUniqueIndex)) {
      return false;
    }
    List<ColumnDefinition> otherColumns = other.getColumns();
    int i = 0, j = 0;
    while (i < columns.size() && j < otherColumns.size()) {
      ColumnDefinition myColumn = columns.get(i);
      ColumnDefinition otherColumn = otherColumns.get(j);
      if (myColumn.getColumnName().equals(otherColumn.getColumnName())) {
        ++i;
        ++j;
      } else {
        return false;
      }
    }
    return true;
  }
}
