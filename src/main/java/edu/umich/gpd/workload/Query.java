package edu.umich.gpd.workload;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public class Query {

  private static int idCount = 1;
  int id;
  private String content;
  private Set<String> columns;
  private Set<String> tables;

  // do not allow using default constructor
  private Query() {

  }

  public Query(String content) {
    this.id = idCount++;
    this.content = content;
    this.columns = new HashSet<>();
    this.tables = new HashSet<>();
  }

  public String getContent() {
    return content;
  }

  public int getId() {
    return id;
  }

  public Set<String> getColumns() {
    return columns;
  }

  public void addColumn(String column) {
    this.columns.add(column);
  }

  public void addTable(String tableName) {
    this.tables.add(tableName);
  }

  public Set<String> getTables() {
    return tables;
  }
}

