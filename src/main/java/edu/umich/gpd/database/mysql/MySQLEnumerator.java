package edu.umich.gpd.database.mysql;

import com.esotericsoftware.minlog.Log;
import com.google.common.collect.Sets;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.database.common.StructureEnumerator;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.schema.Table;
import edu.umich.gpd.parser.InterestingSchemaFinder;
import edu.umich.gpd.util.UniqueNumberGenerator;
import edu.umich.gpd.util.UtilFunctions;
import edu.umich.gpd.workload.Workload;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.*;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public class MySQLEnumerator extends StructureEnumerator {
  @Override
  public List<Set<Structure>> enumerateStructures(Schema s, Workload w) {
    InterestingSchemaFinder finder = new InterestingSchemaFinder();
    finder.getInterestingSchema(w);
    Schema interestingSchema = finder.getFilteredSchema(s);
    if (interestingSchema.isEmpty()) {
      return null;
    } else {
      Set<Structure> structures = new HashSet<>();
      for (Table t : interestingSchema.getTables()) {
        if (!t.isColumnsEmpty()) {
          Set<ColumnDefinition> columns = t.getColumns();
          if (columns.size() > 30) {
            Log.error(this.getClass().getCanonicalName(), "Too many interesting columns." +
                " The number must be less than 31. The current number is " + columns.size());
            return null;
          }
          Set<Set<ColumnDefinition>> columnPowerSet = Sets.powerSet(columns);
          for (Set<ColumnDefinition> columnSet : columnPowerSet) {
            if (!columnSet.isEmpty()) {
              MySQLIndex index = new MySQLIndex(
                  t.getName() + "_index_" + UniqueNumberGenerator.getUniqueID(),
                  t);
              index.setColumns(columnSet);
              structures.add(index);
            }
          }
        }
      }

      if (structures.size() > 30) {
        Log.error(this.getClass().getCanonicalName(), "Too many interesting structures." +
            " It must be less than 31. The current number is " + structures.size());
        return null;
      }

      // now structures have indexes for each table, now we need a powerset from those..
      List<Set<String>> interestingTableSets = finder.getInterestingTableSets();
      Set<Set<Structure>> configurations = new HashSet<>();

      // only get combinations of interesting table sets.
      for (Set<String> tableSets : interestingTableSets) {
        if (tableSets.size() > 1) {
          Set<Structure> structureSet = new HashSet<>();
          for (Structure structure : structures) {
            if (tableSets.contains(structure.getTable().getName())) {
              structureSet.add(structure);
            }
          }
          Set<Set<Structure>> configurationPowerSet = Sets.powerSet(structureSet);
          Set<Set<Structure>> configurationPowerSetWithoutDuplicates = new HashSet<>();
          for (Set<Structure> configuration : configurationPowerSet) {
            if (!UtilFunctions.containsStructureWithDuplicateTables(configuration)) {
              configurationPowerSetWithoutDuplicates.add(configuration);
            }
          }
          configurations.addAll(configurationPowerSetWithoutDuplicates);
        }
      }


//      Set<Set<Structure>> configurations = Sets.powerSet(structures);
//      Set<Set<Structure>> duplicateConfigurations = new HashSet<>();
//      for (Set<Structure> configuration : configurations) {
//        if (UtilFunctions.containsStructureWithDuplicateTables(configuration)) {
//          duplicateConfigurations.add(configuration);
//        }
//      }
      List<Set<Structure>> finalConfigurations = new ArrayList<>(configurations);
//      finalConfigurations.removeAll(duplicateConfigurations);
      return finalConfigurations;
    }
  }
}
