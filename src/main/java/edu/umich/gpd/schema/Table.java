package edu.umich.gpd.schema;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.*;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public class Table {
  private String name;
  private Set<ColumnDefinition> columns;

  public Table() {
    name = "";
    columns = new LinkedHashSet<>();
  }

  public Table(String name) {
    this.name = name;
    columns = new LinkedHashSet<>();
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    Table newTable = new Table();
    newTable.setName(this.name);
    for (ColumnDefinition colDef : columns) {
      newTable.addColumn(colDef);
    }
    return newTable;
  }

  @Override
  public String toString() {
    String str = name + ":\n";
    for (ColumnDefinition colDef : columns) {
      str += "\t" + colDef.getColumnName() + ", " + colDef.getColDataType().getDataType() + "\n";
    }
    return str;
  }

  /**
   * filters uninteresting columns.
   * @param columnNameSet a set of interesting column names
   */
  public void filterUninteresting(Set<String> columnNameSet) {
    Set<ColumnDefinition> newColumns = new LinkedHashSet<>();
    for (ColumnDefinition colDef : columns) {
      if (columnNameSet.contains(colDef.getColumnName())) {
        newColumns.add(colDef);
      }
    }
    columns = newColumns;
  }

  public boolean isColumnsEmpty() {
    return columns.isEmpty();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Set<ColumnDefinition> getColumns() {
    return columns;
  }

  public void addColumn(ColumnDefinition column) {
    columns.add(column);
  }

  public void setColumns(List<ColumnDefinition> columns) {
    columns.addAll(columns);
  }
}
