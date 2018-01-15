package edu.umich.gpd.database.hive;

import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.main.GPDMain;
import edu.umich.gpd.schema.Table;
import edu.umich.gpd.userinput.DatabaseInfo;
import edu.umich.gpd.util.GPDLogger;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.apache.hadoop.fs.Path;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Created by Dong Young Yoon on 1/12/18. */
public class HiveCompactIndex extends Structure {

  private HiveFileType fileType;

  public HiveCompactIndex(String name, Table table, HiveFileType fileType) {
    super(name, table);
    this.fileType = fileType;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = result * prime + table.getName().hashCode();
    try {
      result = result + prime + fileType.getString().hashCode();
    } catch (Exception e) {
      GPDLogger.error(this, "Unknown Hive file type exception caught.");
      e.printStackTrace();
      return -1;
    }
    for (ColumnDefinition cd : columns) {
      result = result * prime + cd.getColumnName().hashCode();
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof HiveCompactIndex) {
      HiveCompactIndex other = (HiveCompactIndex) o;
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
  public boolean create(Connection conn, String dbName) {
    String columnStr = "";
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
      GPDLogger.debug(
          this,
          "Executed: "
              + String.format(
                  "CREATE INDEX %s ON %s (%s) AS 'COMPACT' WITH DEFERRED REBUILD STORED AS %s @ %s",
                  this.name, table.getName(), columnStr, fileType.getString(), targetDBName));
      stmt.execute(
          String.format(
              "CREATE INDEX %s ON %s (%s) AS 'COMPACT' WITH DEFERRED REBUILD STORED AS %s",
              this.name, table.getName(), columnStr, fileType.getString()));
      GPDLogger.debug(
          this,
          "Executed :" + String.format("ALTER INDEX %s ON %s REBUILD", this.name, table.getName()));
      stmt.execute(String.format("ALTER INDEX %s ON %s REBUILD", this.name, table.getName()));

      // Get Size
      DatabaseInfo dbInfo = GPDMain.userInput.getDatabaseInfo();
      long size =
          GPDMain.hadoopFS
              .getFileStatus(
                  new Path(
                      String.format(
                          "/%s/%s.db/%s__%s_%s__",
                          dbInfo.getHiveHDFSPath(),
                          dbInfo.getTargetDBName(),
                          dbInfo.getTargetDBName(),
                          table.getName(),
                          this.name)))
              .getLen();

      this.sizeMap.put(dbName, size);
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  @Override
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

  @Override
  public boolean isCovering(Structure other) {
    if (!(other instanceof HiveCompactIndex)) {
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

  @Override
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
    return String.format(
        "CREATE INDEX %s ON %s (%s) AS 'COMPACT' STORED AS %s;",
        this.name, table.getName(), columnStr, fileType.getString());
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
    return String.format(
        "CREATE INDEX ON %s (%s) AS 'COMPACT' STORED AS %s;",
        table.getName(), columnStr, fileType.getString());
  }
}
