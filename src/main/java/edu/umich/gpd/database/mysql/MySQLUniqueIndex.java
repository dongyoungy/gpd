package edu.umich.gpd.database.mysql;

import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.schema.Table;
import edu.umich.gpd.util.GPDLogger;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public class MySQLUniqueIndex extends Structure {
  private Set<ColumnDefinition> columns;

  public MySQLUniqueIndex(String name, Table table) {
    super(name, table);
    this.columns = new LinkedHashSet<>();
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
    return String.format("CREATE UNIQUE INDEX %s ON %s (%s);", this.name, table.getName(), columnStr);
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
    return String.format("CREATE UNIQUE INDEX %s ON %s (%s);", this.name, table.getName(), columnStr);
  }

  public void addColumn(ColumnDefinition column) {
    this.columns.add(column);
  }

  public void setColumns(Set<ColumnDefinition> columns) {
    this.columns = new LinkedHashSet<>(columns);
  }

  public boolean create(Connection conn) {
    String columnStr = "";
    //ColumnDefinition[] cols = (ColumnDefinition[])columns.toArray();
    List<ColumnDefinition> columnList = new ArrayList<>(columns);
    for (int i = 0; i < columnList.size(); ++i) {
      columnStr += columnList.get(i).getColumnName();
      if (i < columnList.size() - 1) {
        columnStr += ",";
      }
    }
    try {
      Statement stmt = conn.createStatement();
      stmt.execute(String.format("CREATE UNIQUE INDEX %s ON %s (%s)", this.name, table.getName(), columnStr));

      String dbName = conn.getCatalog();
      ResultSet res = stmt.executeQuery(String.format("SELECT stat_value*@@innodb_page_size FROM " +
          "mysql.innodb_index_stats WHERE stat_name = 'size' and database_name = '%s' and " +
          "index_name = '%s'", dbName, this.name));
      if (res.next()) {
        this.size = res.getLong(1);
      }
      else {
        GPDLogger.info(this, "Failed to obtain the size of this physical " +
            "structure: " + name);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean drop(Connection conn) {
    try {
      Statement stmt = conn.createStatement();
      stmt.execute(String.format("DROP INDEX %s ON %s", this.name, table.getName()));
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }
}