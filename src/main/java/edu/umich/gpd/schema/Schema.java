package edu.umich.gpd.schema;

import java.util.*;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class Schema {
  private List<Table> tables;
  private Map<String, Table> tableMap;

  public Schema() {
    tables = new ArrayList<>();
    tableMap = new HashMap<>();
  }

  public boolean isEmpty() {
    return tables.isEmpty();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    Schema newSchema = new Schema();
    for (Table t : tables) {
      newSchema.addTable((Table)t.clone());
    }
    return newSchema;
  }

  public List<Table> getTables() {
    return tables;
  }

  public Table getTable(String name) {
    return tableMap.get(name);
  }
  public void addTable(Table table) {
    tables.add(table);
    tableMap.put(table.getName(), table);
  }

  public void filterUninteresting(Set<String> tableNameSet, Set<String> columnNameSet) {
    List<Table> newTables = new ArrayList<>();
    for (Table t : tables) {

      // if table name not found in the workload, skip it.
      if (!tableNameSet.contains(t.getName())) {
        continue;
      }

      try {
        Table filteredTable = (Table)t.clone();

        // filter uninteresting columns from the table
        filteredTable.filterUninteresting(columnNameSet);
        if (!filteredTable.isColumnsEmpty()) {
          newTables.add(filteredTable);
        }
      } catch (CloneNotSupportedException e) {
        e.printStackTrace();
      }
    }
    tables = newTables;
  }
}
