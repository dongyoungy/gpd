package edu.umich.gpd.database.hive;

import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import edu.umich.gpd.database.common.Configuration;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.database.common.StructureEnumerator;
import edu.umich.gpd.main.GPDMain;
import edu.umich.gpd.parser.InterestingSchemaFinder;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.schema.Table;
import edu.umich.gpd.util.GPDLogger;
import edu.umich.gpd.util.UniqueNumberGenerator;
import edu.umich.gpd.workload.Query;
import edu.umich.gpd.workload.Workload;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import java.util.*;

/** Created by Dong Young Yoon on 2/13/17. */
public class HiveEnumerator extends StructureEnumerator {

  @Override
  public Set<Configuration> enumerateStructures(Schema s, Workload w) {
    return this.enumerateStructuresWithRestrictedSets(s, w);
  }

  @Override
  public Set<Structure> getStructures(Schema s, Workload w) {
    Set<Structure> structures = new LinkedHashSet<>();
    InterestingSchemaFinder finder = new InterestingSchemaFinder();
    if (!finder.getInterestingSchema(w)) {
      return null;
    }
    Set<String> feasibleColumnNameSet = finder.getFeasibleColumnNameSet();
    SortedSet<Configuration> configurations = new TreeSet<>();

    // For each query, generate sets of structures that are 'interesting'
    for (Query q : w.getQueries()) {

      Map<String, Set<List<Structure>>> tableToStructureSetMap = new HashMap<>();
      Set<String> interestingTableSet = q.getTables();
      Set<String> interestingColumnList = q.getColumns();
      Set<Structure> interestingStructures = new LinkedHashSet<>();
      List<Set<Structure>> structuresForQuery = new ArrayList<>();

      for (String tableName : interestingTableSet) {
        Set<Structure> structuresForTable = new HashSet<>();
        Table t = s.getTable(tableName);
        Set<ColumnDefinition> columnsToAdd = new HashSet<>();

        for (ColumnDefinition cd : t.getColumns()) {
          if (interestingColumnList.contains(cd.getColumnName())
              && feasibleColumnNameSet.contains(cd.getColumnName())) {
            columnsToAdd.add(cd);
          }
        }
        if (columnsToAdd.size() > 30) {
          GPDLogger.error(
              this,
              "Too many interesting columns for a query."
                  + " The number must be less than 31. The current number is "
                  + columnsToAdd.size());
          return null;
        }

        Structure structure;
        Set<Set<ColumnDefinition>> columnPowerSet = new HashSet<>();

        for (int i = 1;
             i <= GPDMain.userInput.getSetting().getMaxNumColumnPerStructure()
                 && i <= columnsToAdd.size();
             ++i) {
          columnPowerSet.addAll(Sets.combinations(columnsToAdd, i));
        }
        for (Set<ColumnDefinition> columnSet : columnPowerSet) {
          Collection<List<ColumnDefinition>> perms = Collections2.permutations(columnSet);
          Set<String> columnSetString = new LinkedHashSet<>();
          for (ColumnDefinition cd : columnSet) {
            columnSetString.add(cd.getColumnName());
          }
          for (List<ColumnDefinition> perm : perms) {
            if (perm.isEmpty()) {
              continue;
            }

            for (HiveFileType fileType : HiveFileType.values()) {
              structure =
                  new HiveBitmapIndex(
                      String.format(
                          "%s_bitmap_index_%s_%d",
                          t.getName(), fileType.getString(), UniqueNumberGenerator.getUniqueID()),
                      t,
                      fileType);
              structure.setColumns(perm);
              structures.add(structure);

              structure =
                  new HiveCompactIndex(
                      String.format(
                          "%s_compact_index_%s_%d",
                          t.getName(), fileType.getString(), UniqueNumberGenerator.getUniqueID()),
                      t,
                      fileType);
              structure.setColumns(perm);
              structures.add(structure);
            }
          }
        }
      }
    }
    return structures;
  }

  private Set<Configuration> enumerateStructuresWithRestrictedSets(Schema s, Workload w) {
    InterestingSchemaFinder finder = new InterestingSchemaFinder();
    if (!finder.getInterestingSchema(w)) {
      return null;
    }
    Set<String> feasibleColumnNameSet = finder.getFeasibleColumnNameSet();
    SortedSet<Configuration> configurations = new TreeSet<>();

    // For each query, generate sets of structures that are 'interesting'
    for (Query q : w.getQueries()) {

      Map<String, Set<List<Structure>>> tableToStructureSetMap = new HashMap<>();
      Set<String> interestingTableSet = q.getTables();
      Set<String> interestingColumnList = q.getColumns();
      Set<Structure> interestingStructures = new LinkedHashSet<>();
      List<Set<Structure>> structuresForQuery = new ArrayList<>();

      for (String tableName : interestingTableSet) {
        Set<Structure> structuresForTable = new HashSet<>();
        Table t = s.getTable(tableName);
        Set<ColumnDefinition> columnsToAdd = new HashSet<>();

        for (ColumnDefinition cd : t.getColumns()) {
          if (interestingColumnList.contains(cd.getColumnName())
              && feasibleColumnNameSet.contains(cd.getColumnName())) {
            columnsToAdd.add(cd);
          }
        }
        if (columnsToAdd.size() > 30) {
          GPDLogger.error(
              this,
              "Too many interesting columns for a query."
                  + " The number must be less than 31. The current number is "
                  + columnsToAdd.size());
          return null;
        }

        Structure structure;
        Set<Set<ColumnDefinition>> columnPowerSet = new HashSet<>();

        for (int i = 1;
            i <= GPDMain.userInput.getSetting().getMaxNumColumnPerStructure()
                && i <= columnsToAdd.size();
            ++i) {
          columnPowerSet.addAll(Sets.combinations(columnsToAdd, i));
        }
        for (Set<ColumnDefinition> columnSet : columnPowerSet) {
          Collection<List<ColumnDefinition>> perms = Collections2.permutations(columnSet);
          Set<String> columnSetString = new LinkedHashSet<>();
          for (ColumnDefinition cd : columnSet) {
            columnSetString.add(cd.getColumnName());
          }
          for (List<ColumnDefinition> perm : perms) {
            if (perm.isEmpty()) {
              continue;
            }

            for (HiveFileType fileType : HiveFileType.values()) {
              structure =
                  new HiveBitmapIndex(
                      String.format(
                          "%s_bitmap_index_%s_%d",
                          t.getName(), fileType.getString(), UniqueNumberGenerator.getUniqueID()),
                      t,
                      fileType);
              structure.setColumns(perm);
              interestingStructures.add(structure);
              structuresForTable.add(structure);

              structure =
                  new HiveCompactIndex(
                      String.format(
                          "%s_compact_index_%s_%d",
                          t.getName(), fileType.getString(), UniqueNumberGenerator.getUniqueID()),
                      t,
                      fileType);
              structure.setColumns(perm);
              interestingStructures.add(structure);
              structuresForTable.add(structure);
            }
          }
        }

        // Identify structure sets for a table.
        Set<List<Structure>> structureSetsForTable = new HashSet<>();
        ICombinatoricsVector<Structure> initVector = Factory.createVector(structuresForTable);
        int maxStructuresPerTable = GPDMain.userInput.getSetting().getMaxNumStructurePerTable();
        int numStructureToConsider =
            (initVector.getSize() > maxStructuresPerTable)
                ? maxStructuresPerTable
                : initVector.getSize();
        Generator<Structure> gen =
            Factory.createSimpleCombinationGenerator(initVector, numStructureToConsider);
        for (ICombinatoricsVector<Structure> comb : gen) {
          List<Structure> combList = comb.getVector();
          // For now, only consider the max # structure per table = 2
          if (combList.size() > 1) {
            if (!combList.get(0).isCovering(combList.get(1))) {
              structureSetsForTable.add(combList);
            }
          } else {
            structureSetsForTable.add(combList);
          }
        }
        tableToStructureSetMap.put(tableName, structureSetsForTable);
      }

      ICombinatoricsVector<String> tableVector = Factory.createVector(interestingTableSet);
      int maxNumTablePerQuery = GPDMain.userInput.getSetting().getMaxNumTablePerQuery();
      int numTableToConsider =
          (tableVector.getSize() > maxNumTablePerQuery)
              ? maxNumTablePerQuery
              : tableVector.getSize();
      Generator<String> gen =
          Factory.createSimpleCombinationGenerator(tableVector, numTableToConsider);

      // For a chosen set of tables, enumerate possible set of structures.
      for (ICombinatoricsVector<String> tableSet : gen) {
        List<String> tableList = tableSet.getVector();
        List<Set<List<Structure>>> structureSetListFromChosenTables = new ArrayList<>();
        for (String table : tableList) {
          Set<List<Structure>> setsForTable = tableToStructureSetMap.get(table);
          structureSetListFromChosenTables.add(setsForTable);
        }
        Set<List<List<Structure>>> cartesianSets =
            Sets.cartesianProduct(structureSetListFromChosenTables);
        for (List<List<Structure>> sets : cartesianSets) {
          List<Structure> aConfig = new ArrayList<>();
          for (List<Structure> set : sets) {
            aConfig.addAll(set);
          }
          Configuration newConfig = new Configuration(new ArrayList(aConfig));
          configurations.add(newConfig);
          q.addConfiguration(newConfig);
        }
      }
      Configuration emptyConfig = new Configuration(new ArrayList<Structure>());
      configurations.add(emptyConfig);
      q.addConfiguration(emptyConfig);
    }

    return configurations;
  }
}
