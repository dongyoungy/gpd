package edu.umich.gpd.workload;

import edu.umich.gpd.database.common.Configuration;
import edu.umich.gpd.database.common.Structure;

import java.util.*;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public class Query {

  private static int idCount = 1;
  int id;
  private String content;
  private Set<String> columns;
  private Set<String> tables;
  private Set<Configuration> configurations;

  // do not allow using default constructor
  private Query() {

  }

  public Query(String content) {
    this.id = idCount++;
    this.content = content;
    this.columns = new HashSet<>();
    this.tables = new HashSet<>();
    this.configurations = new LinkedHashSet<>();
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

  public Set<Configuration> getConfigurations() {
    return configurations;
  }

  public void addConfiguration(Configuration configuration) {
    this.configurations.add(configuration);
  }

  public Set<String> getTables() {
    return tables;
  }

  public List<Configuration> getConfigurationList() {
    return new ArrayList<>(configurations);
  }
}

