package edu.umich.gpd.schema;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.*;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public class Table {
  private String name;
  private String createStatement;
  private Set<ColumnDefinition> columns;
  private Set<String> indexedColumns;

  public Table() {
    name = "";
    columns = new LinkedHashSet<>();
    indexedColumns = new LinkedHashSet<>();
  }

  public Table(String name) {
    this.name = name;
    columns = new LinkedHashSet<>();
    indexedColumns = new LinkedHashSet<>();
  }

  public Table(String name, String createStatement) {
    this.name = name;
    this.createStatement = createStatement;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    Table newTable = new Table();
    newTable.setName(this.name);
    newTable.setCreateStatement(this.createStatement);
    for (ColumnDefinition colDef : columns) {
      newTable.addColumn(colDef);
    }
    for (String c : indexedColumns) {
      newTable.addIndexedColumn(c);
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
   * filters uninteresting columns + already indexed columns.
   * @param columnNameSet a set of interesting column names
   */
  public void filterUninteresting(Set<String> columnNameSet) {
    Set<ColumnDefinition> newColumns = new LinkedHashSet<>();
    for (ColumnDefinition colDef : columns) {
      if (columnNameSet.contains(colDef.getColumnName()) &&
          !indexedColumns.contains(colDef.getColumnName())) {
        newColumns.add(colDef);
      }
    }
    columns = newColumns;
  }

  public void setCreateStatement(String createStatement) {
    this.createStatement = createStatement;
  }

  public String getCreateStatement() {
    return createStatement;
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

  public void addIndexedColumn(String column) {
    indexedColumns.add(column);
  }
}
