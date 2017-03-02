package edu.umich.gpd.database.common;

import edu.umich.gpd.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public abstract class Structure {
  protected String name;
  protected Table table;
  protected long size;
  protected List<ColumnDefinition> columns;
  protected int id;

  private static int idCount = 1;

  public Structure() {

  }

  public Structure(String name, Table table) {
    this.name = name;
    this.table = table;
    this.size = -1;
    this.columns = new ArrayList<>();
    this.id = idCount++;
  }

  public int getId() {
    return id;
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

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object o);

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

  public abstract boolean isCovering(Structure other);

  public abstract String getQueryString();

  public void addColumn(ColumnDefinition column) {
    this.columns.add(column);
  }

  public void setColumns(List<ColumnDefinition> columns) {
    this.columns = new ArrayList<>(columns);
  }

  public List<ColumnDefinition> getColumns() {
    return columns;
  }
}
