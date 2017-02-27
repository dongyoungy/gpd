package edu.umich.gpd.database.common;

import edu.umich.gpd.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public abstract class Structure {
  protected String name;
  protected Table table;
  protected long size;
  protected Set<ColumnDefinition> columns;

  public Structure() {

  }

  public Structure(String name, Table table) {
    this.name = name;
    this.table = table;
    this.size = -1;
  }

  public String getName() {
    return name;
  }

  public Table getTable() {
    return table;
  }

  public long getSize() {
    return size;
  }

  /**
   * creates this physical structure using the provided JDBC connection
   * @param conn JDBC connection to the target database
   * @return true if the creation is successful, false otherwise.
   */
  public abstract boolean create(Connection conn);

  /**
   * delete/drops this physical structure using the provided JDBC connection
   * @param conn JDBC connection to the target database
   * @return true if the deletion is successful, false otherwise.
   */
  public abstract boolean drop(Connection conn);

  public abstract String getQueryString();

  public void addColumn(ColumnDefinition column) {
    this.columns.add(column);
  }

  public void setColumns(Set<ColumnDefinition> columns) {
    this.columns = new LinkedHashSet<>(columns);
  }

}
