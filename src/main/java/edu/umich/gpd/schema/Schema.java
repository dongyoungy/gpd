package edu.umich.gpd.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class Schema {
  private List<Table> tables;

  public Schema() {
    tables = new ArrayList<>();
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

  public void addTable(Table table) {
    tables.add(table);
  }

  public void filterUninteresting(Set<String> tableNameSet, Set<String> columnNameSet) {
    List<Table> newTables = new ArrayList<>();
    for (Table t : tables) {
      try
      {
        Table filteredTable = (Table)t.clone();

        // interesting table
        if (tableNameSet.contains(filteredTable.getName())) {
          filteredTable.filterUninteresting(columnNameSet);
          if (!filteredTable.isColumnsEmpty()) {
            newTables.add(filteredTable);
          }
        }
      }
      catch (CloneNotSupportedException e)
      {
        e.printStackTrace();
      }
    }
    tables = newTables;
  }
}
