package edu.umich.gpd.database.mysql;

import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import edu.umich.gpd.database.common.Configuration;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.database.common.StructureEnumerator;
import edu.umich.gpd.main.GPDMain;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.schema.Table;
import edu.umich.gpd.parser.InterestingSchemaFinder;
import edu.umich.gpd.util.GPDLogger;
import edu.umich.gpd.util.UniqueNumberGenerator;
import edu.umich.gpd.util.UtilFunctions;
import edu.umich.gpd.workload.Query;
import edu.umich.gpd.workload.Workload;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.*;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public class MySQLEnumerator extends StructureEnumerator {

  @Override
  public Set<Configuration> enumerateStructures(Schema s, Workload w) {
    InterestingSchemaFinder finder = new InterestingSchemaFinder();
    if (!finder.getInterestingSchema(w))  {
      return null;
    }

    Set<String> feasibleColumnNameSet = finder.getFeasibleColumnNameSet();
    Set<Configuration> configurations = new HashSet<>();

    // add empty set first
//    configurations.add(new Configuration());

    // For each query, generate structures that are 'interesting'
    for (Query q : w.getQueries()) {

      Set<String> interestingTableSet = q.getTables();
      Set<String> interestingColumnList = q.getColumns();
      Set<Structure> interestingStructures = new LinkedHashSet<>();
      List<Set<Structure>> structuresForQuery = new ArrayList<>();

      for (String tableName : interestingTableSet) {
        Set<Structure> structuresForTable = new HashSet<>();
        Table t = s.getTable(tableName);
        Set<ColumnDefinition> columnsToAdd = new HashSet<>();
        for (ColumnDefinition cd : t.getColumns()) {
          if (interestingColumnList.contains(cd.getColumnName()) &&
              feasibleColumnNameSet.contains(cd.getColumnName())) {
            columnsToAdd.add(cd);
          }
        }
        if (columnsToAdd.size() > 30) {
          GPDLogger.error(this, "Too many interesting columns for a query." +
              " The number must be less than 31. The current number is " + columnsToAdd.size());
          return null;
        }

        Structure structure;
        if (columnsToAdd.size() > GPDMain.userInput.getSetting().getMaxNumColumnPerStructure()) {
          for (ColumnDefinition cd : columnsToAdd) {
            if (t.getPrimaryKeys().contains(cd.getColumnName()) && t.getPrimaryKeys().size() == 1) {
              structure = new MySQLUniqueIndex(
                  t.getName() + "_unique_index_" + UniqueNumberGenerator.getUniqueID(),
                  t);
            } else {
              structure = new MySQLIndex(
                  t.getName() + "_index_" + UniqueNumberGenerator.getUniqueID(),
                  t);
            }
            ArrayList<ColumnDefinition> structureColumns = new ArrayList<>();
            structureColumns.add(cd);
            structure.setColumns(structureColumns);
            interestingStructures.add(structure);
          }
        } else {
          Set<Set<ColumnDefinition>> columnPowerSet = Sets.powerSet(columnsToAdd);
          for (Set<ColumnDefinition> columnSet : columnPowerSet) {
            if (columnSet.size() > GPDMain.userInput.getSetting().getMaxNumColumnPerStructure()) {
              continue;
            }
            Collection<List<ColumnDefinition>> perms = Collections2.permutations(columnSet);
            Set<String> columnSetString = new LinkedHashSet<>();
            for (ColumnDefinition cd : columnSet) {
              columnSetString.add(cd.getColumnName());
            }
            for (List<ColumnDefinition> perm : perms) {
              if (perm.isEmpty()) {
                continue;
              }
              if (Sets.symmetricDifference(t.getPrimaryKeys(), columnSetString).isEmpty()) {
                structure = new MySQLUniqueIndex(
                    t.getName() + "_unique_index_" + UniqueNumberGenerator.getUniqueID(),
                    t);
              } else {
                structure = new MySQLIndex(
                    t.getName() + "_index_" + UniqueNumberGenerator.getUniqueID(),
                    t);
              }
              structure.setColumns(perm);
              interestingStructures.add(structure);
            }
          }
        }
      }
      if (interestingStructures.size() > 30) {
        GPDLogger.warn(this, "Too many interesting structures." +
            " It must be less than 31. The current number is " + interestingStructures.size());
        return null;
      }
      Set<Set<Structure>> structurePowersets = Sets.powerSet(interestingStructures);
      for (Set<Structure> config : structurePowersets) {
        Configuration newConfig = new Configuration(new ArrayList(config));
        configurations.add(newConfig);
        q.addConfiguration(newConfig);
      }
    }

    return configurations;
  }

  public List<Set<Structure>> enumerateStructuresOld(Schema s, Workload w) {
    InterestingSchemaFinder finder = new InterestingSchemaFinder();
    if (!finder.getInterestingSchema(w))  {
      return null;
    }
    Schema interestingSchema = finder.getFilteredSchema(s);
    if (interestingSchema.isEmpty()) {
      return null;
    } else {
      Set<Structure> structures = new HashSet<>();
      for (Table t : interestingSchema.getTables()) {
        if (!t.isColumnsEmpty()) {

          Set<ColumnDefinition> columns = t.getColumns();
          Structure structure = null;
          // TESTING: if t has more columns than X, then we only consider single column indexes
          // from t
          if (columns.size() > 3) {
            for (ColumnDefinition cd : columns) {
              if (t.getPrimaryKeys().contains(cd.getColumnName())) {
                structure = new MySQLUniqueIndex(
                    t.getName() + "_unique_index_" + UniqueNumberGenerator.getUniqueID(),
                    t);
              } else {
                structure = new MySQLIndex(
                    t.getName() + "_index_" + UniqueNumberGenerator.getUniqueID(),
                    t);
              }
              ArrayList<ColumnDefinition> structureColumns = new ArrayList<>();
              structureColumns.add(cd);
              structure.setColumns(structureColumns);
              structures.add(structure);
            }
          } else {
            for (Query q : w.getQueries()) {
              Set<ColumnDefinition> columnsToAdd = new HashSet<>();
              for (ColumnDefinition col : columns) {
                if (q.getColumns().contains(col.getColumnName())) {
                  columnsToAdd.add(col);
                }
              }
              if (columnsToAdd.size() > 30) {
                GPDLogger.error(this, "Too many interesting columns for a query." +
                    " The number must be less than 31. The current number is " +
                    columnsToAdd.size());
                return null;
              }
              Set<Set<ColumnDefinition>> columnPowerSet = Sets.powerSet(columnsToAdd);
              for (Set<ColumnDefinition> columnSet : columnPowerSet) {
                if (!columnSet.isEmpty() && columnSet.size() <= 3) {
                  Set<String> columnSetString = new LinkedHashSet<>();
                  for (ColumnDefinition cd : columnSet) {
                    columnSetString.add(cd.getColumnName());
                  }

                  Collection<List<ColumnDefinition>> perms = Collections2.permutations(columnSet);
                  for (List<ColumnDefinition> perm : perms) {
                    if (Sets.symmetricDifference(t.getPrimaryKeys(), columnSetString).isEmpty()) {
                      structure = new MySQLUniqueIndex(
                          t.getName() + "_unique_index_" + UniqueNumberGenerator.getUniqueID(),
                          t);
                    } else {
                      structure = new MySQLIndex(
                          t.getName() + "_index_" + UniqueNumberGenerator.getUniqueID(),
                          t);
                    }
                    structure.setColumns(perm);
                    structures.add(structure);
                  }
                }
              }
            }
          }
        }
      }

      // now structures have indexes for each table, now we need a powerset from those..
      List<Set<String>> interestingTableSets = finder.getInterestingTableSets();
      Set<Set<Structure>> configurations = new HashSet<>();

      Set<Structure> structureSet = new HashSet<>();
      for (Structure structure : structures) {
        for (Set<String> tableSets : interestingTableSets) {
          if (tableSets.contains(structure.getTable().getName())) {
            boolean isCovered = false;
            boolean canCover = false;
            Structure coveredStructure = null;
            for (Structure st : structureSet) {
              if (st.isCovering(structure)) {
                isCovered = true;
                break;
              } else if (structure.isCovering(st)) {
                canCover = true;
                coveredStructure = st;
                break;
              }
            }
            if (!isCovered) {
              structureSet.add(structure);
              break;
            } else if (canCover) {
              structureSet.remove(coveredStructure);
              structureSet.add(structure);
              break;
            }
          }
        }
      }
//      Set<Set<Structure>> configurationPowerSet = getPowerSet(structureSet);

      if (structureSet.size() > 30) {
        GPDLogger.warn(this, "Too many interesting structures." +
            " It must be less than 31. The current number is " + structureSet.size());
        return null;
      }
      Set<Set<Structure>> configurationPowerSet = Sets.powerSet(structureSet);
      if (configurationPowerSet == null) {
        return null;
      }
      Set<Set<Structure>> configurationPowerSetWithoutDuplicates = new HashSet<>();
      for (Set<Structure> configuration : configurationPowerSet) {
        if (!UtilFunctions.containsStructureWithDuplicateTables(configuration)) {
          configurationPowerSetWithoutDuplicates.add(configuration);
        }
      }
      configurations.addAll(configurationPowerSetWithoutDuplicates);

      // only get combinations of interesting table sets.
//      for (Set<String> tableSets : interestingTableSets) {
//        if (!tableSets.isEmpty()) {
//          Set<Structure> structureSet = new HashSet<>();
//          for (Structure structure : structures) {
//            if (tableSets.contains(structure.getTable().getName())) {
//              structureSet.add(structure);
//            }
//          }
//          if (structureSet.size() > 30) {
//            GPDLogger.error(this, "Too many interesting structures." +
//                " It must be less than 31. The current number is " + structureSet.size());
//            return null;
//          }
//
//          Set<Set<Structure>> configurationPowerSet = Sets.powerSet(structureSet);
//          Set<Set<Structure>> configurationPowerSetWithoutDuplicates = new HashSet<>();
//          for (Set<Structure> configuration : configurationPowerSet) {
//            if (!UtilFunctions.containsStructureWithDuplicateTables(configuration)) {
//              configurationPowerSetWithoutDuplicates.add(configuration);
//            }
//          }
//          configurations.addAll(configurationPowerSetWithoutDuplicates);
//        }
//      }


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

  private Set<Set<Structure>> getPowerSet(Set<Structure> set) {
    Set<String> tableNameSet = new HashSet<>();
    Map<String, Integer> tableDuplicateCount = new HashMap<>();
    for (Structure s : set) {
      tableNameSet.add(s.getTable().getName());
    }
    for (String tableName : tableNameSet) {
      tableDuplicateCount.put(tableName, 0);
    }
    for (Structure s : set) {
      tableDuplicateCount.put(s.getTable().getName(),
          tableDuplicateCount.get(s.getTable().getName()).intValue()+1);
    }
    int mostDuplicatedTableCount = 0;
    String mostDuplicatedTableName = "";
    for (Map.Entry<String, Integer> entry : tableDuplicateCount.entrySet()) {
      if (mostDuplicatedTableCount < entry.getValue().intValue()) {
        mostDuplicatedTableCount = entry.getValue().intValue();
        mostDuplicatedTableName = entry.getKey();
      }
    }

    Set<Structure> duplicatedStructures = new HashSet<>();
    if (mostDuplicatedTableCount > 0) {
      for (Structure s : set) {
        if (s.getTable().getName().equals(mostDuplicatedTableName)) {
          duplicatedStructures.add(s);
        }
      }
    }
    set.removeAll(duplicatedStructures);

    Set<Set<Structure>> powerSet = new HashSet<>();

    // now combine set + duplicatedStructures to build powerset
    for (Structure s : duplicatedStructures) {
      Set<Structure> newSet = new HashSet<>(set);
      newSet.add(s);

      if (newSet.size() > 30) {
        GPDLogger.warn(this, "Too many interesting structures." +
            " It must be less than 31. The current number is " + newSet.size());
        return null;
      }
      powerSet.addAll(Sets.powerSet(newSet));
    }

    return powerSet;
  }

}
