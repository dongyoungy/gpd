package edu.umich.gpd.database.mysql;

import edu.umich.gpd.database.common.FeatureExtractor;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.schema.Table;
import edu.umich.gpd.userinput.SampleInfo;
import edu.umich.gpd.util.GPDLogger;
import edu.umich.gpd.workload.Query;
import edu.umich.gpd.workload.Workload;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dong Young Yoon on 2/20/17.
 */
public class MySQLFeatureExtractor extends FeatureExtractor {

  public MySQLFeatureExtractor(Connection conn) {
    super(conn);
  }

  @Override
  public boolean initialize(List<SampleInfo> sampleDBs, String targetDBName, Schema s) {

    ArrayList<Attribute> attrList = new ArrayList<>();
    try {
      for (Table t : s.getTables()) {
        if (sampleDBs != null) {
          for (SampleInfo sample : sampleDBs) {
            String dbName = sample.getDbName();
            conn.setCatalog(dbName);
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", t.getName()));
            if (!res.next()) {
              GPDLogger.error(this, String.format("Failed to the row counts from table '%s'",
                  t.getName()));
              return false;
            } else {
              Long count = res.getLong(1);
              t.addRowCount(dbName, count.longValue());
            }
          }
        } else {
          GPDLogger.error(this, "There are no sample databases to extract " +
              "features.");
          return false;
        }
        attrList.add(new Attribute("numRow_" + t.getName()));
//
//        String dbName = targetDBName;
//        conn.setCatalog(dbName);
//        Statement stmt = conn.createStatement();
//        ResultSet res = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", t.getName()));
//        if (!res.next()) {
//          GPDLogger.error(this, String.format("Failed to the row counts from table '%s'",
//              t.getName()));
//          return false;
//        } else {
//          Long count = res.getLong(1);
//          t.addRowCount(dbName, count.longValue());
//          attrList.add(new Attribute("numRow" + t.getName()));
//        }
      }

    } catch (SQLException e) {
      GPDLogger.error(this, "SQLException has been caught.");
      e.printStackTrace();
      return false;
    }
    attrList.add(new Attribute("queryId"));
    attrList.add(new Attribute("configId"));
    attrList.add(new Attribute("totalRowFromSimpleSelect"));
    attrList.add(new Attribute("totalRowFromPrimarySelect"));
    attrList.add(new Attribute("totalRowFromUnionSelect"));
    attrList.add(new Attribute("totalRowFromDependentUnionSelect"));
    attrList.add(new Attribute("totalRowFromUnionResultSelect"));
    attrList.add(new Attribute("totalRowFromSubquerySelect"));
    attrList.add(new Attribute("totalRowFromDependentSubquerySelect"));
    attrList.add(new Attribute("totalRowFromDerivedSelect"));
    attrList.add(new Attribute("totalRowFromMaterializedSelect"));
    attrList.add(new Attribute("totalRowFromUncacheableSubquerySelect"));
    attrList.add(new Attribute("totalRowFromUncacheableUnionSelect"));
    attrList.add(new Attribute("numJoinTypeSystem"));
    attrList.add(new Attribute("numJoinTypeConst"));
    attrList.add(new Attribute("numJoinTypeEqRef"));
    attrList.add(new Attribute("numJoinTypeRef"));
    attrList.add(new Attribute("numJoinTypeFulltext"));
    attrList.add(new Attribute("numJoinTypeRefOrNull"));
    attrList.add(new Attribute("numJoinTypeIndexMerge"));
    attrList.add(new Attribute("numJoinTypeUniqueSubquery"));
    attrList.add(new Attribute("numJoinTypeIndexSubquery"));
    attrList.add(new Attribute("numJoinTypeRange"));
    attrList.add(new Attribute("numJoinTypeIndex"));
    attrList.add(new Attribute("numJoinTypeAll"));
    attrList.add(new Attribute("numExtraChildPushedJoin"));
    attrList.add(new Attribute("numExtraConstRowNotFound"));
    attrList.add(new Attribute("numExtraDeletingAllRows"));
    attrList.add(new Attribute("numExtraDistinct"));
    attrList.add(new Attribute("numExtraFirstMatch"));
    attrList.add(new Attribute("numExtraFullScanOnNullKey"));
    attrList.add(new Attribute("numExtraImpossibleHaving"));
    attrList.add(new Attribute("numExtraImpossibleWhere"));
    attrList.add(new Attribute("numExtraImpossibleWhereNoticed"));
    attrList.add(new Attribute("numExtraLooseScan"));
    attrList.add(new Attribute("numExtraNoMatchingMinMaxRow"));
    attrList.add(new Attribute("numExtraNoMatchingRowInConstTable"));
    attrList.add(new Attribute("numExtraNoMatchingRowsAfterPartitionPruning"));
    attrList.add(new Attribute("numExtraNoTablesUsed"));
    attrList.add(new Attribute("numExtraNotExists"));
    attrList.add(new Attribute("numExtraPlanNotReadyYet"));
    attrList.add(new Attribute("numExtraRangeChecked"));
    attrList.add(new Attribute("numExtraSelecTablesOptimizedAway"));
    attrList.add(new Attribute("numExtraStartTemporary"));
    attrList.add(new Attribute("numExtraEndTemporary"));
    attrList.add(new Attribute("numExtraUniqueRowNotFound"));
    attrList.add(new Attribute("numExtraUsingFilesort"));
    attrList.add(new Attribute("numExtraUsingIndex"));
    attrList.add(new Attribute("numExtraUsingIndexCondition"));
    attrList.add(new Attribute("numExtraUsingIndexForGroupBy"));
    attrList.add(new Attribute("numExtraUsingJoinBuffer"));
    attrList.add(new Attribute("numExtraUsingMRR"));
    attrList.add(new Attribute("numExtraUsingSortUnion"));
    attrList.add(new Attribute("numExtraUsingUnion"));
    attrList.add(new Attribute("numExtraUsingIntersect"));
    attrList.add(new Attribute("numExtraUsingTemporary"));
    attrList.add(new Attribute("numExtraUsingWhere"));
    attrList.add(new Attribute("numExtraZeroLimit"));
    attrList.add(new Attribute("queryTime"));

    trainData = new Instances("trainData", attrList, 1000);
    trainData.setClassIndex(trainData.numAttributes() - 1);
    return true;
  }

  @Override
  public boolean addTrainingData(String dbName, Schema s, Query q, int configIndex,
                                 double queryTime) {

    int queryId = q.getId();
    long totalRowFromSimpleSelect = 0;
    long totalRowFromPrimarySelect = 0;
    long totalRowFromUnionSelect = 0;
    long totalRowFromDependentUnionSelect = 0;
    long totalRowFromUnionResultSelect = 0;
    long totalRowFromSubquerySelect = 0;
    long totalRowFromDependentSubquerySelect = 0;
    long totalRowFromDerivedSelect = 0;
    long totalRowFromMaterializedSelect = 0;
    long totalRowFromUncacheableSubquerySelect = 0;
    long totalRowFromUncacheableUnionSelect = 0;
    long numJoinTypeSystem = 0;
    long numJoinTypeConst = 0;
    long numJoinTypeEqRef = 0;
    long numJoinTypeRef = 0;
    long numJoinTypeFulltext = 0;
    long numJoinTypeRefOrNull = 0;
    long numJoinTypeIndexMerge = 0;
    long numJoinTypeUniqueSubquery = 0;
    long numJoinTypeIndexSubquery = 0;
    long numJoinTypeRange = 0;
    long numJoinTypeIndex = 0;
    long numJoinTypeAll = 0;
    long numExtraChildPushedJoin = 0;
    long numExtraConstRowNotFound = 0;
    long numExtraDeletingAllRows = 0;
    long numExtraDistinct = 0;
    long numExtraFirstMatch = 0;
    long numExtraFullScanOnNullKey = 0;
    long numExtraImpossibleHaving = 0;
    long numExtraImpossibleWhere = 0;
    long numExtraImpossibleWhereNoticed = 0;
    long numExtraLooseScan = 0;
    long numExtraNoMatchingMinMaxRow = 0;
    long numExtraNoMatchingRowInConstTable = 0;
    long numExtraNoMatchingRowsAfterPartitionPruning = 0;
    long numExtraNoTablesUsed = 0;
    long numExtraNotExists = 0;
    long numExtraPlanNotReadyYet = 0;
    long numExtraRangeChecked = 0;
    long numExtraSelecTablesOptimizedAway = 0;
    long numExtraStartTemporary = 0;
    long numExtraEndTemporary = 0;
    long numExtraUniqueRowNotFound = 0;
    long numExtraUsingFilesort = 0;
    long numExtraUsingIndex = 0;
    long numExtraUsingIndexCondition = 0;
    long numExtraUsingIndexForGroupBy = 0;
    long numExtraUsingJoinBuffer = 0;
    long numExtraUsingMRR = 0;
    long numExtraUsingSortUnion = 0;
    long numExtraUsingUnion = 0;
    long numExtraUsingIntersect = 0;
    long numExtraUsingTemporary = 0;
    long numExtraUsingWhere = 0;
    long numExtraZeroLimit = 0;

    try {
      conn.setCatalog(dbName);
      Statement stmt = conn.createStatement();
      ResultSet res = stmt.executeQuery(String.format("EXPLAIN EXTENDED %s", q.getContent()));
      while (res.next()) {
        String selectType = res.getString("select_type");
        long numRows = res.getLong("rows");
        double filtered = res.getDouble("filtered");
        String extra = res.getString("Extra");
        String joinType = res.getString("type");

        // extract # of rows from select
        numRows = (long) ((numRows) * (filtered / 100.0));
        switch (selectType.toUpperCase()) {
          case "SIMPLE":
            totalRowFromSimpleSelect += numRows;
            break;
          case "PRIMARY":
            totalRowFromPrimarySelect += numRows;
            break;
          case "UNION":
            totalRowFromUnionSelect += numRows;
            break;
          case "DEPENDENT UNION":
            totalRowFromDependentUnionSelect += numRows;
            break;
          case "UNION RESULT":
            totalRowFromUnionResultSelect += numRows;
            break;
          case "SUBQUERY":
            totalRowFromSubquerySelect += numRows;
            break;
          case "DEPENDENT SUBQUERY":
            totalRowFromDependentSubquerySelect += numRows;
            break;
          case "DERIVED":
            totalRowFromDerivedSelect += numRows;
            break;
          case "MATERIALIZED":
            totalRowFromMaterializedSelect += numRows;
            break;
          case "UNCACHEABLE SUBQUERY":
            totalRowFromUncacheableSubquerySelect += numRows;
            break;
          case "UNCACHEABLE UNION":
            totalRowFromUncacheableUnionSelect += numRows;
            break;
          default:
            GPDLogger.error(this, "Unsupported Select type: " + selectType);
            break;
        }

        // extract features on join types
        switch (joinType.toLowerCase()) {
          case "system":
            numJoinTypeSystem++;
            break;
          case "const":
            numJoinTypeConst++;
            break;
          case "eq_ref":
            numJoinTypeEqRef++;
            break;
          case "ref":
            numJoinTypeRef++;
            break;
          case "fulltext":
            numJoinTypeFulltext++;
            break;
          case "ref_or_null":
            numJoinTypeRefOrNull++;
            break;
          case "index_merge":
            numJoinTypeIndexMerge++;
            break;
          case "unique_subquery":
            numJoinTypeUniqueSubquery++;
            break;
          case "index_subquery":
            numJoinTypeIndexSubquery++;
            break;
          case "range":
            numJoinTypeRange++;
            break;
          case "index":
            numJoinTypeIndex++;
            break;
          case "all":
            numJoinTypeAll++;
            break;
          default:
            GPDLogger.error(this, "Unsupported Join type: " + joinType);
            break;
        }

        // now processes 'extra' column
        if (extra != null) {
          String lowerExtra = extra.toLowerCase();
          if (lowerExtra.contains("pushed join")) {
            numExtraChildPushedJoin++;
          }
          if (lowerExtra.contains("const row not found")) {
            numExtraConstRowNotFound++;
          }
          if (lowerExtra.contains("deleting all rows")) {
            numExtraDeletingAllRows++;
          }
          if (lowerExtra.contains("distinct")) {
            numExtraDistinct++;
          }
          if (lowerExtra.contains("firstmatch")) {
            numExtraFirstMatch++;
          }
          if (lowerExtra.contains("full scan on null key")) {
            numExtraFullScanOnNullKey++;
          }
          if (lowerExtra.contains("impossible having")) {
            numExtraImpossibleHaving++;
          }
          if (lowerExtra.contains("impossible where") && !lowerExtra.contains("noticed after")) {
            numExtraImpossibleWhere++;
          }
          if (lowerExtra.contains("impossible where noticed after")) {
            numExtraImpossibleWhereNoticed++;
          }
          if (lowerExtra.contains("loosescan")) {
            numExtraLooseScan++;
          }
          if (lowerExtra.contains("no matching min/max")) {
            numExtraNoMatchingMinMaxRow++;
          }
          if (lowerExtra.contains("no matching row in const table")) {
            numExtraNoMatchingRowInConstTable++;
          }
          if (lowerExtra.contains("no matching rows after partition pruning")) {
            numExtraNoMatchingRowsAfterPartitionPruning++;
          }
          if (lowerExtra.contains("no tables used")) {
            numExtraNoTablesUsed++;
          }
          if (lowerExtra.contains("not exists")) {
            numExtraNotExists++;
          }
          if (lowerExtra.contains("plan isn't ready yet")) {
            numExtraPlanNotReadyYet++;
          }
          if (lowerExtra.contains("range checked for each record")) {
            numExtraRangeChecked++;
          }
          if (lowerExtra.contains("select tables optimized away")) {
            numExtraSelecTablesOptimizedAway++;
          }
          if (lowerExtra.contains("start temporary")) {
            numExtraStartTemporary++;
          }
          if (lowerExtra.contains("end temporary")) {
            numExtraEndTemporary++;
          }
          if (lowerExtra.contains("unique row not found")) {
            numExtraUniqueRowNotFound++;
          }
          if (lowerExtra.contains("using filesort")) {
            numExtraUsingFilesort++;
          }
          if (lowerExtra.contains("using index condition")) {
            numExtraUsingIndexCondition++;
          }
          else if (lowerExtra.contains("using index for group-by")) {
            numExtraUsingIndexForGroupBy++;
          }
          else if (lowerExtra.contains("using index")) {
            numExtraUsingIndex++;
          }
          if (lowerExtra.contains("using join buffer")) {
            numExtraUsingJoinBuffer++;
          }
          if (lowerExtra.contains("using mrr")) {
            numExtraUsingMRR++;
          }
          if (lowerExtra.contains("using sort_union")) {
            numExtraUsingSortUnion++;
          }
          if (lowerExtra.contains("using union")) {
            numExtraUsingUnion++;
          }
          if (lowerExtra.contains("using intersect")) {
            numExtraUsingIntersect++;
          }
          if (lowerExtra.contains("using where")) {
            numExtraUsingWhere++;
          }
          if (lowerExtra.contains("zero limit")) {
            numExtraZeroLimit++;
          }
        }

        Instance newInstance = new DenseInstance(trainData.numAttributes());
        int idx = 0;
        for (Table t : s.getTables()) {
          newInstance.setValue(idx++, t.getRowCount(dbName));
        }
        newInstance.setValue(idx++, queryId);
        newInstance.setValue(idx++, configIndex);
        newInstance.setValue(idx++, totalRowFromSimpleSelect);
        newInstance.setValue(idx++, totalRowFromPrimarySelect);
        newInstance.setValue(idx++, totalRowFromUnionSelect);
        newInstance.setValue(idx++, totalRowFromDependentUnionSelect);
        newInstance.setValue(idx++, totalRowFromUnionResultSelect);
        newInstance.setValue(idx++, totalRowFromSubquerySelect);
        newInstance.setValue(idx++, totalRowFromDependentSubquerySelect);
        newInstance.setValue(idx++, totalRowFromDerivedSelect);
        newInstance.setValue(idx++, totalRowFromMaterializedSelect);
        newInstance.setValue(idx++, totalRowFromUncacheableSubquerySelect);
        newInstance.setValue(idx++, totalRowFromUncacheableUnionSelect);
        newInstance.setValue(idx++, numJoinTypeSystem);
        newInstance.setValue(idx++, numJoinTypeConst);
        newInstance.setValue(idx++, numJoinTypeEqRef);
        newInstance.setValue(idx++, numJoinTypeRef);
        newInstance.setValue(idx++, numJoinTypeFulltext);
        newInstance.setValue(idx++, numJoinTypeRefOrNull);
        newInstance.setValue(idx++, numJoinTypeIndexMerge);
        newInstance.setValue(idx++, numJoinTypeUniqueSubquery);
        newInstance.setValue(idx++, numJoinTypeIndexSubquery);
        newInstance.setValue(idx++, numJoinTypeRange);
        newInstance.setValue(idx++, numJoinTypeIndex);
        newInstance.setValue(idx++, numJoinTypeAll);
        newInstance.setValue(idx++, numExtraChildPushedJoin);
        newInstance.setValue(idx++, numExtraConstRowNotFound);
        newInstance.setValue(idx++, numExtraDeletingAllRows);
        newInstance.setValue(idx++, numExtraDistinct);
        newInstance.setValue(idx++, numExtraFirstMatch);
        newInstance.setValue(idx++, numExtraFullScanOnNullKey);
        newInstance.setValue(idx++, numExtraImpossibleHaving);
        newInstance.setValue(idx++, numExtraImpossibleWhere);
        newInstance.setValue(idx++, numExtraImpossibleWhereNoticed);
        newInstance.setValue(idx++, numExtraLooseScan);
        newInstance.setValue(idx++, numExtraNoMatchingMinMaxRow);
        newInstance.setValue(idx++, numExtraNoMatchingRowInConstTable);
        newInstance.setValue(idx++, numExtraNoMatchingRowsAfterPartitionPruning);
        newInstance.setValue(idx++, numExtraNoTablesUsed);
        newInstance.setValue(idx++, numExtraNotExists);
        newInstance.setValue(idx++, numExtraPlanNotReadyYet);
        newInstance.setValue(idx++, numExtraRangeChecked);
        newInstance.setValue(idx++, numExtraSelecTablesOptimizedAway);
        newInstance.setValue(idx++, numExtraStartTemporary);
        newInstance.setValue(idx++, numExtraEndTemporary);
        newInstance.setValue(idx++, numExtraUniqueRowNotFound);
        newInstance.setValue(idx++, numExtraUsingFilesort);
        newInstance.setValue(idx++, numExtraUsingIndex);
        newInstance.setValue(idx++, numExtraUsingIndexCondition);
        newInstance.setValue(idx++, numExtraUsingIndexForGroupBy);
        newInstance.setValue(idx++, numExtraUsingJoinBuffer);
        newInstance.setValue(idx++, numExtraUsingMRR);
        newInstance.setValue(idx++, numExtraUsingSortUnion);
        newInstance.setValue(idx++, numExtraUsingUnion);
        newInstance.setValue(idx++, numExtraUsingIntersect);
        newInstance.setValue(idx++, numExtraUsingTemporary);
        newInstance.setValue(idx++, numExtraUsingWhere);
        newInstance.setValue(idx++, numExtraZeroLimit);
        newInstance.setValue(idx++, queryTime);

        trainData.add(newInstance);
      }
    } catch (SQLException e) {
      GPDLogger.error(this, "SQLException has been caught.");
      e.printStackTrace();
      return false;
    }
    return true;
  }

  @Override
  public Instance getTestInstance(String dbName, Schema s, Query q, int configIndex) {

    int queryId = q.getId();
    long totalRowFromSimpleSelect = 0;
    long totalRowFromPrimarySelect = 0;
    long totalRowFromUnionSelect = 0;
    long totalRowFromDependentUnionSelect = 0;
    long totalRowFromUnionResultSelect = 0;
    long totalRowFromSubquerySelect = 0;
    long totalRowFromDependentSubquerySelect = 0;
    long totalRowFromDerivedSelect = 0;
    long totalRowFromMaterializedSelect = 0;
    long totalRowFromUncacheableSubquerySelect = 0;
    long totalRowFromUncacheableUnionSelect = 0;
    long numJoinTypeSystem = 0;
    long numJoinTypeConst = 0;
    long numJoinTypeEqRef = 0;
    long numJoinTypeRef = 0;
    long numJoinTypeFulltext = 0;
    long numJoinTypeRefOrNull = 0;
    long numJoinTypeIndexMerge = 0;
    long numJoinTypeUniqueSubquery = 0;
    long numJoinTypeIndexSubquery = 0;
    long numJoinTypeRange = 0;
    long numJoinTypeIndex = 0;
    long numJoinTypeAll = 0;
    long numExtraChildPushedJoin = 0;
    long numExtraConstRowNotFound = 0;
    long numExtraDeletingAllRows = 0;
    long numExtraDistinct = 0;
    long numExtraFirstMatch = 0;
    long numExtraFullScanOnNullKey = 0;
    long numExtraImpossibleHaving = 0;
    long numExtraImpossibleWhere = 0;
    long numExtraImpossibleWhereNoticed = 0;
    long numExtraLooseScan = 0;
    long numExtraNoMatchingMinMaxRow = 0;
    long numExtraNoMatchingRowInConstTable = 0;
    long numExtraNoMatchingRowsAfterPartitionPruning = 0;
    long numExtraNoTablesUsed = 0;
    long numExtraNotExists = 0;
    long numExtraPlanNotReadyYet = 0;
    long numExtraRangeChecked = 0;
    long numExtraSelecTablesOptimizedAway = 0;
    long numExtraStartTemporary = 0;
    long numExtraEndTemporary = 0;
    long numExtraUniqueRowNotFound = 0;
    long numExtraUsingFilesort = 0;
    long numExtraUsingIndex = 0;
    long numExtraUsingIndexCondition = 0;
    long numExtraUsingIndexForGroupBy = 0;
    long numExtraUsingJoinBuffer = 0;
    long numExtraUsingMRR = 0;
    long numExtraUsingSortUnion = 0;
    long numExtraUsingUnion = 0;
    long numExtraUsingIntersect = 0;
    long numExtraUsingTemporary = 0;
    long numExtraUsingWhere = 0;
    long numExtraZeroLimit = 0;

    try {
      conn.setCatalog(dbName);
      Statement stmt = conn.createStatement();
      ResultSet res = stmt.executeQuery(String.format("EXPLAIN EXTENDED %s", q.getContent()));
      while (res.next()) {
        String selectType = res.getString("select_type");
        long numRows = res.getLong("rows");
        double filtered = res.getDouble("filtered");
        String extra = res.getString("extra");
        String joinType = res.getString("type");

        // extract # of rows from select
        numRows = (long) ((numRows) * (filtered / 100.0));
        switch (selectType.toUpperCase()) {
          case "SIMPLE":
            totalRowFromSimpleSelect += numRows;
            break;
          case "PRIMARY":
            totalRowFromPrimarySelect += numRows;
            break;
          case "UNION":
            totalRowFromUnionSelect += numRows;
            break;
          case "DEPENDENT UNION":
            totalRowFromDependentUnionSelect += numRows;
            break;
          case "UNION RESULT":
            totalRowFromUnionResultSelect += numRows;
            break;
          case "SUBQUERY":
            totalRowFromSubquerySelect += numRows;
            break;
          case "DEPENDENT SUBQUERY":
            totalRowFromDependentSubquerySelect += numRows;
            break;
          case "DERIVED":
            totalRowFromDerivedSelect += numRows;
            break;
          case "MATERIALIZED":
            totalRowFromMaterializedSelect += numRows;
            break;
          case "UNCACHEABLE SUBQUERY":
            totalRowFromUncacheableSubquerySelect += numRows;
            break;
          case "UNCACHEABLE UNION":
            totalRowFromUncacheableUnionSelect += numRows;
            break;
          default:
            GPDLogger.error(this, "Unsupported Select type: " + selectType);
            break;
        }

        // extract features on join types
        switch (joinType.toLowerCase()) {
          case "system":
            numJoinTypeSystem++;
            break;
          case "const":
            numJoinTypeConst++;
            break;
          case "eq_ref":
            numJoinTypeEqRef++;
            break;
          case "ref":
            numJoinTypeRef++;
            break;
          case "fulltext":
            numJoinTypeFulltext++;
            break;
          case "ref_or_null":
            numJoinTypeRefOrNull++;
            break;
          case "index_merge":
            numJoinTypeIndexMerge++;
            break;
          case "unique_subquery":
            numJoinTypeUniqueSubquery++;
            break;
          case "index_subquery":
            numJoinTypeIndexSubquery++;
            break;
          case "range":
            numJoinTypeRange++;
            break;
          case "index":
            numJoinTypeIndex++;
            break;
          case "all":
            numJoinTypeAll++;
            break;
          default:
            GPDLogger.error(this, "Unsupported Join type: " + joinType);
            break;
        }

        // now processes 'extra' column
        String lowerExtra = extra.toLowerCase();
        if (lowerExtra.contains("pushed join")) {
          numExtraChildPushedJoin++;
        }
        if (lowerExtra.contains("const row not found")) {
          numExtraConstRowNotFound++;
        }
        if (lowerExtra.contains("deleting all rows")) {
          numExtraDeletingAllRows++;
        }
        if (lowerExtra.contains("distinct")) {
          numExtraDistinct++;
        }
        if (lowerExtra.contains("firstmatch")) {
          numExtraFirstMatch++;
        }
        if (lowerExtra.contains("full scan on null key")) {
          numExtraFullScanOnNullKey++;
        }
        if (lowerExtra.contains("impossible having")) {
          numExtraImpossibleHaving++;
        }
        if (lowerExtra.contains("impossible where") && !lowerExtra.contains("noticed after")) {
          numExtraImpossibleWhere++;
        }
        if (lowerExtra.contains("impossible where noticed after")) {
          numExtraImpossibleWhereNoticed++;
        }
        if (lowerExtra.contains("loosescan")) {
          numExtraLooseScan++;
        }
        if (lowerExtra.contains("no matching min/max")) {
          numExtraNoMatchingMinMaxRow++;
        }
        if (lowerExtra.contains("no matching row in const table")) {
          numExtraNoMatchingRowInConstTable++;
        }
        if (lowerExtra.contains("no matching rows after partition pruning")) {
          numExtraNoMatchingRowsAfterPartitionPruning++;
        }
        if (lowerExtra.contains("no tables used")) {
          numExtraNoTablesUsed++;
        }
        if (lowerExtra.contains("not exists")) {
          numExtraNotExists++;
        }
        if (lowerExtra.contains("plan isn't ready yet")) {
          numExtraPlanNotReadyYet++;
        }
        if (lowerExtra.contains("range checked for each record")) {
          numExtraRangeChecked++;
        }
        if (lowerExtra.contains("select tables optimized away")) {
          numExtraSelecTablesOptimizedAway++;
        }
        if (lowerExtra.contains("start temporary")) {
          numExtraStartTemporary++;
        }
        if (lowerExtra.contains("end temporary")) {
          numExtraEndTemporary++;
        }
        if (lowerExtra.contains("unique row not found")) {
          numExtraUniqueRowNotFound++;
        }
        if (lowerExtra.contains("using filesort")) {
          numExtraUsingFilesort++;
        }
        if (lowerExtra.contains("using index condition")) {
          numExtraUsingIndexCondition++;
        } else if (lowerExtra.contains("using index for group-by")) {
          numExtraUsingIndexForGroupBy++;
        } else if (lowerExtra.contains("using index")) {
          numExtraUsingIndex++;
        }
        if (lowerExtra.contains("using join buffer")) {
          numExtraUsingJoinBuffer++;
        }
        if (lowerExtra.contains("using mrr")) {
          numExtraUsingMRR++;
        }
        if (lowerExtra.contains("using sort_union")) {
          numExtraUsingSortUnion++;
        }
        if (lowerExtra.contains("using union")) {
          numExtraUsingUnion++;
        }
        if (lowerExtra.contains("using intersect")) {
          numExtraUsingIntersect++;
        }
        if (lowerExtra.contains("using where")) {
          numExtraUsingWhere++;
        }
        if (lowerExtra.contains("zero limit")) {
          numExtraZeroLimit++;
        }

        // 57 features + query time + configuration id + index size
//        Instance newInstance = new DenseInstance(57 + 1);
        Instance newInstance = new DenseInstance(trainData.numAttributes());
        int idx = 0;
        for (Table t : s.getTables()) {
          newInstance.setValue(idx++, t.getRowCount(dbName));
        }
        newInstance.setValue(idx++, queryId);
        newInstance.setValue(idx++, configIndex);
        newInstance.setValue(idx++, totalRowFromSimpleSelect);
        newInstance.setValue(idx++, totalRowFromPrimarySelect);
        newInstance.setValue(idx++, totalRowFromUnionSelect);
        newInstance.setValue(idx++, totalRowFromDependentUnionSelect);
        newInstance.setValue(idx++, totalRowFromUnionResultSelect);
        newInstance.setValue(idx++, totalRowFromSubquerySelect);
        newInstance.setValue(idx++, totalRowFromDependentSubquerySelect);
        newInstance.setValue(idx++, totalRowFromDerivedSelect);
        newInstance.setValue(idx++, totalRowFromMaterializedSelect);
        newInstance.setValue(idx++, totalRowFromUncacheableSubquerySelect);
        newInstance.setValue(idx++, totalRowFromUncacheableUnionSelect);
        newInstance.setValue(idx++, numJoinTypeSystem);
        newInstance.setValue(idx++, numJoinTypeConst);
        newInstance.setValue(idx++, numJoinTypeEqRef);
        newInstance.setValue(idx++, numJoinTypeRef);
        newInstance.setValue(idx++, numJoinTypeFulltext);
        newInstance.setValue(idx++, numJoinTypeRefOrNull);
        newInstance.setValue(idx++, numJoinTypeIndexMerge);
        newInstance.setValue(idx++, numJoinTypeUniqueSubquery);
        newInstance.setValue(idx++, numJoinTypeIndexSubquery);
        newInstance.setValue(idx++, numJoinTypeRange);
        newInstance.setValue(idx++, numJoinTypeIndex);
        newInstance.setValue(idx++, numJoinTypeAll);
        newInstance.setValue(idx++, numExtraChildPushedJoin);
        newInstance.setValue(idx++, numExtraConstRowNotFound);
        newInstance.setValue(idx++, numExtraDeletingAllRows);
        newInstance.setValue(idx++, numExtraDistinct);
        newInstance.setValue(idx++, numExtraFirstMatch);
        newInstance.setValue(idx++, numExtraFullScanOnNullKey);
        newInstance.setValue(idx++, numExtraImpossibleHaving);
        newInstance.setValue(idx++, numExtraImpossibleWhere);
        newInstance.setValue(idx++, numExtraImpossibleWhereNoticed);
        newInstance.setValue(idx++, numExtraLooseScan);
        newInstance.setValue(idx++, numExtraNoMatchingMinMaxRow);
        newInstance.setValue(idx++, numExtraNoMatchingRowInConstTable);
        newInstance.setValue(idx++, numExtraNoMatchingRowsAfterPartitionPruning);
        newInstance.setValue(idx++, numExtraNoTablesUsed);
        newInstance.setValue(idx++, numExtraNotExists);
        newInstance.setValue(idx++, numExtraPlanNotReadyYet);
        newInstance.setValue(idx++, numExtraRangeChecked);
        newInstance.setValue(idx++, numExtraSelecTablesOptimizedAway);
        newInstance.setValue(idx++, numExtraStartTemporary);
        newInstance.setValue(idx++, numExtraEndTemporary);
        newInstance.setValue(idx++, numExtraUniqueRowNotFound);
        newInstance.setValue(idx++, numExtraUsingFilesort);
        newInstance.setValue(idx++, numExtraUsingIndex);
        newInstance.setValue(idx++, numExtraUsingIndexCondition);
        newInstance.setValue(idx++, numExtraUsingIndexForGroupBy);
        newInstance.setValue(idx++, numExtraUsingJoinBuffer);
        newInstance.setValue(idx++, numExtraUsingMRR);
        newInstance.setValue(idx++, numExtraUsingSortUnion);
        newInstance.setValue(idx++, numExtraUsingUnion);
        newInstance.setValue(idx++, numExtraUsingIntersect);
        newInstance.setValue(idx++, numExtraUsingTemporary);
        newInstance.setValue(idx++, numExtraUsingWhere);
        newInstance.setValue(idx++, numExtraZeroLimit);

        return newInstance;
      }
    } catch (SQLException e) {
      GPDLogger.error(this, "SQLException has been caught.");
      e.printStackTrace();
      return null;
    }
    return null;
  }
}