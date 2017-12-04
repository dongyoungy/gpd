package edu.umich.gpd.workload;

import edu.umich.gpd.database.common.Configuration;
import edu.umich.gpd.database.common.Structure;

import java.util.*;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public class Query implements Comparable {

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Query query = (Query) o;

    if (id != query.id) return false;
    return content != null ? content.equals(query.content) : query.content == null;
  }

  @Override
  public int hashCode() {
    int result = id;
    result = 31 * result + (content != null ? content.hashCode() : 0);
    return result;
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

  @Override
  public int compareTo(Object o) {
    Query other = (Query) o;
    if (id < other.id) return -1;
    else if (id > other.id) return 1;
    else return 0;
  }
}

