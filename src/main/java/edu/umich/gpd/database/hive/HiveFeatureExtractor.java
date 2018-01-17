package edu.umich.gpd.database.hive;

import edu.umich.gpd.database.common.FeatureExtractor;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.schema.Table;
import edu.umich.gpd.userinput.SampleInfo;
import edu.umich.gpd.util.GPDLogger;
import edu.umich.gpd.workload.Query;
import weka.core.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/** Created by Dong Young Yoon on 2/20/17. */
public class HiveFeatureExtractor extends FeatureExtractor {

  private Set<Structure> structureSet;
  private List<String> structureStrList;

  public HiveFeatureExtractor(Connection conn) {
    super(conn);
  }

  @Override
  public boolean initialize(
      List<SampleInfo> sampleDBs,
      String targetDBName,
      Schema s,
      Set<Structure> structureSet,
      List<String> structureStrList) {

    this.structureSet = new LinkedHashSet<>(structureSet);
    this.structureStrList = new ArrayList<>(structureStrList);
    ArrayList<Attribute> attrList = new ArrayList<>();
    ArrayList<Attribute> attrListForSize = new ArrayList<>();
    try {
      attrListForSize.add(new Attribute("numRow"));
      if (sampleDBs != null) {
        for (SampleInfo sample : sampleDBs) {
          String dbName = sample.getDbName();
          for (Table t : s.getTables()) {
            Statement stmt = conn.createStatement();
            stmt.execute("USE " + dbName);
            ResultSet res =
                stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", t.getName()));
            if (!res.next()) {
              GPDLogger.error(
                  this, String.format("Failed to the row counts from table '%s'", t.getName()));
              return false;
            } else {
              Long count = res.getLong(1);
              t.addRowCount(dbName, count);
            }
            res.close();
          }
        }
      } else {
        GPDLogger.error(this, "There are no sample databases to extract " + "features.");
        return false;
      }
      for (Table t : s.getTables()) {
        attrList.add(new Attribute("numRow_" + t.getName()));
      }
    } catch (SQLException e) {
      GPDLogger.error(this, "SQLException has been caught.");
      e.printStackTrace();
      return false;
    }
    attrList.add(new Attribute("queryId"));
    attrList.add(new Attribute("configId"));
    attrList.add(new Attribute("totalRowFromTableScan"));
    attrList.add(new Attribute("totalRowFromSelect"));
    attrList.add(new Attribute("totalRowFromFilter"));
    attrList.add(new Attribute("totalRowFromGroupBy"));
    attrList.add(new Attribute("totalRowFromJoin"));
    attrList.add(new Attribute("totalRowFromMapJoin"));
    attrList.add(new Attribute("totalRowFromMergeJoin"));
    attrList.add(new Attribute("totalRowFromReduceOutput"));
    for (int i = 0; i < structureStrList.size(); ++i) {
      attrList.add(new Attribute("structure_" + i));
    }
    attrList.add(new Attribute("queryTime"));
    attrListForSize.add(new Attribute("structureStr", structureStrList));
    attrListForSize.add(new Attribute("distinctRowCount"));
    attrListForSize.add(new Attribute("structureSize"));

    trainData = new Instances("trainData", attrList, 10000);
    trainDataForSize = new Instances("trainDataForSize", attrListForSize, 10000);
    return true;
  }

  @Override
  public boolean addTrainingData(
      String dbName, Schema s, Query q, List<String> structures, double queryTime) {

    int queryId = q.getId();
    long totalRowFromTableScan = 0;
    long totalRowFromSelect = 0;
    long totalRowFromFilter = 0;
    long totalRowFromGroupBy = 0;
    long totalRowFromJoin = 0;
    long totalRowFromMapJoin = 0;
    long totalRowFromMergeJoin = 0;
    long totalRowFromReduceOutput = 0;

    try {
      Statement stmt = conn.createStatement();
      stmt.execute("USE " + dbName);
      ResultSet res = stmt.executeQuery(String.format("EXPLAIN EXTENDED %s", q.getContent()));

      String lastOperator = "";
      Map<String, Long> operatorRowMap = new HashMap<>();

      // Parse Explain Output.
      while (res.next()) {
        String explainText = res.getString(1);
        StringTokenizer tokenizer = new StringTokenizer(explainText, "\n");
        while (tokenizer.hasMoreTokens()) {
          String line = tokenizer.nextToken().trim();
          String[] words = line.split("\\s+");
          if (line.contains("Operator") || line.contains("TableScan")) {
            lastOperator = line;
          }
          if (words[0].equals("Statistics:")) {
            long currentRow = 0;
            if (!operatorRowMap.containsKey(lastOperator)) {
              operatorRowMap.put(lastOperator, 0L);
            } else {
              currentRow = operatorRowMap.get(lastOperator);
            }
            currentRow += Long.parseLong(words[3]);
            operatorRowMap.put(lastOperator, currentRow);
          }
        }
      }

      for (Map.Entry<String, Long> entry : operatorRowMap.entrySet()) {
        String op = entry.getKey();
        long count = entry.getValue();

        switch (op) {
          case "TableScan":
            totalRowFromTableScan = count;
            break;
          case "Select Operator":
            totalRowFromSelect = count;
            break;
          case "Filter Operator":
            totalRowFromFilter = count;
            break;
          case "Group By Operator":
            totalRowFromGroupBy = count;
            break;
          case "Join Operator":
            totalRowFromJoin = count;
            break;
          case "Map Join Operator":
            totalRowFromMapJoin = count;
            break;
          case "Merge Join Operator":
            totalRowFromMergeJoin = count;
            break;
          case "Reduce Output Operator":
            totalRowFromReduceOutput = count;
            break;
          default:
            GPDLogger.error(this, "Unknown operator: " + op);
            return false;
        }
      }

      Instance newInstance = new SparseInstance(trainData.numAttributes());
      newInstance.setDataset(trainData);
      int idx = 0;
      for (Table t : s.getTables()) {
        newInstance.setValue(idx++, t.getRowCount(dbName));
      }
      newInstance.setValue(idx++, queryId);
      newInstance.setValue(idx++, 0);
      newInstance.setValue(idx++, totalRowFromTableScan);
      newInstance.setValue(idx++, totalRowFromSelect);
      newInstance.setValue(idx++, totalRowFromFilter);
      newInstance.setValue(idx++, totalRowFromGroupBy);
      newInstance.setValue(idx++, totalRowFromJoin);
      newInstance.setValue(idx++, totalRowFromMapJoin);
      newInstance.setValue(idx++, totalRowFromMergeJoin);
      newInstance.setValue(idx++, totalRowFromReduceOutput);
      for (String structureStr : structureStrList) {
        if (structures.contains(structureStr)) {
          newInstance.setValue(idx++, 1);
        } else {
          newInstance.setValue(idx++, 0);
        }
      }
      newInstance.setValue(idx++, queryTime);

      trainData.add(newInstance);
    } catch (SQLException e) {
      GPDLogger.error(this, "SQLException has been caught.");
      e.printStackTrace();
      return false;
    }
    return true;
  }

  @Override
  public boolean addTrainingData(String dbName, Schema s, Query q, int configId, double queryTime) {

    int queryId = q.getId();
    long totalRowFromTableScan = 0;
    long totalRowFromSelect = 0;
    long totalRowFromFilter = 0;
    long totalRowFromGroupBy = 0;
    long totalRowFromJoin = 0;
    long totalRowFromMapJoin = 0;
    long totalRowFromMergeJoin = 0;
    long totalRowFromReduceOutput = 0;

    try {
      Statement stmt = conn.createStatement();
      stmt.execute("USE " + dbName);
      ResultSet res = stmt.executeQuery(String.format("EXPLAIN EXTENDED %s", q.getContent()));

      String lastOperator = "";
      Map<String, Long> operatorRowMap = new HashMap<>();

      // Parse Explain Output.
      while (res.next()) {
        String explainText = res.getString(1);
        StringTokenizer tokenizer = new StringTokenizer(explainText, "\n");
        while (tokenizer.hasMoreTokens()) {
          String line = tokenizer.nextToken().trim();
          String[] words = line.split("\\s+");
          if (line.contains("Operator") || line.contains("TableScan")) {
            lastOperator = line;
          }
          if (words[0].equals("Statistics:")) {
            long currentRow = 0;
            if (!operatorRowMap.containsKey(lastOperator)) {
              operatorRowMap.put(lastOperator, 0L);
            } else {
              currentRow = operatorRowMap.get(lastOperator);
            }
            currentRow += Long.parseLong(words[3]);
            operatorRowMap.put(lastOperator, currentRow);
          }
        }
      }

      for (Map.Entry<String, Long> entry : operatorRowMap.entrySet()) {
        String op = entry.getKey();
        long count = entry.getValue();

        switch (op) {
          case "TableScan":
            totalRowFromTableScan = count;
            break;
          case "Select Operator":
            totalRowFromSelect = count;
            break;
          case "Filter Operator":
            totalRowFromFilter = count;
            break;
          case "Group By Operator":
            totalRowFromGroupBy = count;
            break;
          case "Join Operator":
            totalRowFromJoin = count;
            break;
          case "Map Join Operator":
            totalRowFromMapJoin = count;
            break;
          case "Merge Join Operator":
            totalRowFromMergeJoin = count;
            break;
          case "Reduce Output Operator":
            totalRowFromReduceOutput = count;
            break;
          default:
            GPDLogger.error(this, "Unknown operator: " + op);
            return false;
        }
      }

      Instance newInstance = new SparseInstance(trainData.numAttributes());
      newInstance.setDataset(trainData);
      int idx = 0;
      for (Table t : s.getTables()) {
        newInstance.setValue(idx++, t.getRowCount(dbName));
      }
      newInstance.setValue(idx++, queryId);
      newInstance.setValue(idx++, configId);
      newInstance.setValue(idx++, totalRowFromTableScan);
      newInstance.setValue(idx++, totalRowFromSelect);
      newInstance.setValue(idx++, totalRowFromFilter);
      newInstance.setValue(idx++, totalRowFromGroupBy);
      newInstance.setValue(idx++, totalRowFromJoin);
      newInstance.setValue(idx++, totalRowFromMapJoin);
      newInstance.setValue(idx++, totalRowFromMergeJoin);
      newInstance.setValue(idx++, totalRowFromReduceOutput);
      for (String structureStr : structureStrList) {
        newInstance.setValue(idx++, 0);
      }
      newInstance.setValue(idx++, queryTime);

      trainData.add(newInstance);
    } catch (SQLException e) {
      GPDLogger.error(this, "SQLException has been caught.");
      e.printStackTrace();
      return false;
    }
    return true;
  }

  @Override
  public boolean addTrainingDataForSize(String dbName, Schema s, Structure structure) {
    Instance newInstance = new DenseInstance(trainDataForSize.numAttributes());
    newInstance.setDataset(trainDataForSize);
    int idx = 0;
    newInstance.setValue(idx++, structure.getTable().getRowCount(dbName));
    GPDLogger.debug(
        this,
        String.format(
            "Adding row count = %d, distinct row count = %d, size = %d for table %s @ %s",
            structure.getTable().getRowCount(dbName),
            structure.getTable().getDistinctRowCount(dbName, structure.getColumnString()),
            structure.getSize(dbName),
            structure.getTable().getName(),
            dbName));
    newInstance.setValue(idx++, structure.getNonUniqueString());
    newInstance.setValue(
        idx++, structure.getTable().getDistinctRowCount(dbName, structure.getColumnString()));
    newInstance.setValue(idx++, structure.getSize(dbName));
    trainDataForSize.add(newInstance);
    return true;
  }

  @Override
  public Instance getTestInstance(String dbName, Schema s, Query q, int configId) {

    int queryId;
    if (q == null) {
      queryId = -1;
    } else {
      queryId = q.getId();
    }
    long totalRowFromTableScan = 0;
    long totalRowFromSelect = 0;
    long totalRowFromFilter = 0;
    long totalRowFromGroupBy = 0;
    long totalRowFromJoin = 0;
    long totalRowFromMapJoin = 0;
    long totalRowFromMergeJoin = 0;
    long totalRowFromReduceOutput = 0;

    try {
      Statement stmt = conn.createStatement();
      stmt.execute("USE " + dbName);
      ResultSet res = stmt.executeQuery(String.format("EXPLAIN EXTENDED %s", q.getContent()));

      String lastOperator = "";
      Map<String, Long> operatorRowMap = new HashMap<>();

      // Parse Explain Output.
      while (res.next()) {
        String explainText = res.getString(1);
        StringTokenizer tokenizer = new StringTokenizer(explainText, "\n");
        while (tokenizer.hasMoreTokens()) {
          String line = tokenizer.nextToken().trim();
          String[] words = line.split("\\s+");
          if (line.contains("Operator") || line.contains("TableScan")) {
            lastOperator = line;
          }
          if (words[0].equals("Statistics:")) {
            long currentRow = 0;
            if (!operatorRowMap.containsKey(lastOperator)) {
              operatorRowMap.put(lastOperator, 0L);
            } else {
              currentRow = operatorRowMap.get(lastOperator);
            }
            currentRow += Long.parseLong(words[3]);
            operatorRowMap.put(lastOperator, currentRow);
          }
        }
      }

      for (Map.Entry<String, Long> entry : operatorRowMap.entrySet()) {
        String op = entry.getKey();
        long count = entry.getValue();

        switch (op) {
          case "TableScan":
            totalRowFromTableScan = count;
            break;
          case "Select Operator":
            totalRowFromSelect = count;
            break;
          case "Filter Operator":
            totalRowFromFilter = count;
            break;
          case "Group By Operator":
            totalRowFromGroupBy = count;
            break;
          case "Join Operator":
            totalRowFromJoin = count;
            break;
          case "Map Join Operator":
            totalRowFromMapJoin = count;
            break;
          case "Merge Join Operator":
            totalRowFromMergeJoin = count;
            break;
          case "Reduce Output Operator":
            totalRowFromReduceOutput = count;
            break;
          default:
            GPDLogger.error(this, "Unknown operator: " + op);
            return null;
        }
      }

      Instance newInstance = new SparseInstance(trainData.numAttributes());
      newInstance.setDataset(trainData);
      int idx = 0;
      for (Table t : s.getTables()) {
        newInstance.setValue(idx++, t.getRowCount(dbName));
      }
      if (queryId == -1) {
        idx++;
      } else {
        newInstance.setValue(idx++, queryId);
      }
      newInstance.setValue(idx++, configId);
      newInstance.setValue(idx++, totalRowFromTableScan);
      newInstance.setValue(idx++, totalRowFromSelect);
      newInstance.setValue(idx++, totalRowFromFilter);
      newInstance.setValue(idx++, totalRowFromGroupBy);
      newInstance.setValue(idx++, totalRowFromJoin);
      newInstance.setValue(idx++, totalRowFromMapJoin);
      newInstance.setValue(idx++, totalRowFromMergeJoin);
      newInstance.setValue(idx++, totalRowFromReduceOutput);
      for (String structureStr : structureStrList) {
        newInstance.setValue(idx++, 0);
      }

      return newInstance;
    } catch (SQLException e) {
      GPDLogger.error(this, "SQLException has been caught.");
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public Instance getTestInstance(String dbName, Schema s, Query q, List<String> structures) {

    int queryId;
    if (q == null) {
      queryId = -1;
    } else {
      queryId = q.getId();
    }
    long totalRowFromTableScan = 0;
    long totalRowFromSelect = 0;
    long totalRowFromFilter = 0;
    long totalRowFromGroupBy = 0;
    long totalRowFromJoin = 0;
    long totalRowFromMapJoin = 0;
    long totalRowFromMergeJoin = 0;
    long totalRowFromReduceOutput = 0;

    try {
      Statement stmt = conn.createStatement();
      stmt.execute("USE " + dbName);
      ResultSet res = stmt.executeQuery(String.format("EXPLAIN EXTENDED %s", q.getContent()));

      String lastOperator = "";
      Map<String, Long> operatorRowMap = new HashMap<>();

      // Parse Explain Output.
      while (res.next()) {
        String explainText = res.getString(1);
        StringTokenizer tokenizer = new StringTokenizer(explainText, "\n");
        while (tokenizer.hasMoreTokens()) {
          String line = tokenizer.nextToken().trim();
          String[] words = line.split("\\s+");
          if (line.contains("Operator") || line.contains("TableScan")) {
            lastOperator = line;
          }
          if (words[0].equals("Statistics:")) {
            long currentRow = 0;
            if (!operatorRowMap.containsKey(lastOperator)) {
              operatorRowMap.put(lastOperator, 0L);
            } else {
              currentRow = operatorRowMap.get(lastOperator);
            }
            currentRow += Long.parseLong(words[3]);
            operatorRowMap.put(lastOperator, currentRow);
          }
        }
      }

      for (Map.Entry<String, Long> entry : operatorRowMap.entrySet()) {
        String op = entry.getKey();
        long count = entry.getValue();

        switch (op) {
          case "TableScan":
            totalRowFromTableScan = count;
            break;
          case "Select Operator":
            totalRowFromSelect = count;
            break;
          case "Filter Operator":
            totalRowFromFilter = count;
            break;
          case "Group By Operator":
            totalRowFromGroupBy = count;
            break;
          case "Join Operator":
            totalRowFromJoin = count;
            break;
          case "Map Join Operator":
            totalRowFromMapJoin = count;
            break;
          case "Merge Join Operator":
            totalRowFromMergeJoin = count;
            break;
          case "Reduce Output Operator":
            totalRowFromReduceOutput = count;
            break;
          default:
            GPDLogger.error(this, "Unknown operator: " + op);
            return null;
        }
      }

      // 57 features + query time + configuration id + index size
      //        Instance newInstance = new DenseInstance(57 + 1);
      Instance newInstance = new SparseInstance(trainData.numAttributes());
      newInstance.setDataset(trainData);
      int idx = 0;
      for (Table t : s.getTables()) {
        newInstance.setValue(idx++, t.getRowCount(dbName));
      }
      if (queryId == -1) {
        idx++;
      } else {
        newInstance.setValue(idx++, queryId);
      }
      newInstance.setValue(idx++, 0);
      newInstance.setValue(idx++, totalRowFromTableScan);
      newInstance.setValue(idx++, totalRowFromSelect);
      newInstance.setValue(idx++, totalRowFromFilter);
      newInstance.setValue(idx++, totalRowFromGroupBy);
      newInstance.setValue(idx++, totalRowFromJoin);
      newInstance.setValue(idx++, totalRowFromMapJoin);
      newInstance.setValue(idx++, totalRowFromMergeJoin);
      newInstance.setValue(idx++, totalRowFromReduceOutput);
      for (String structureStr : structureStrList) {
        if (structures.contains(structureStr)) {
          newInstance.setValue(idx++, 1);
        } else {
          newInstance.setValue(idx++, 0);
        }
      }
      return newInstance;
    } catch (SQLException e) {
      GPDLogger.error(this, "SQLException has been caught.");
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void clearTrainData() {
    trainData.clear();
  }

  @Override
  public Instance getTestInstanceForSize(String dbName, Schema s, Structure structure) {
    Instance newInstance = new DenseInstance(trainDataForSize.numAttributes());
    newInstance.setDataset(trainDataForSize);
    int idx = 0;
    newInstance.setValue(idx++, structure.getTable().getRowCount(dbName));
    GPDLogger.debug(
        this,
        String.format(
            "Getting row count = %d, distinct count = %d for table %s @ %s",
            structure.getTable().getRowCount(dbName),
            structure.getTable().getDistinctRowCount(dbName, structure.getColumnString()),
            structure.getTable().getName(),
            dbName));
    newInstance.setValue(idx++, structure.getNonUniqueString());
    newInstance.setValue(
        idx++, structure.getTable().getDistinctRowCount(dbName, structure.getColumnString()));
    return newInstance;
  }
}
