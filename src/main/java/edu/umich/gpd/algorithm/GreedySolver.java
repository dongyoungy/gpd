package edu.umich.gpd.algorithm;

import edu.umich.gpd.database.common.Configuration;
import edu.umich.gpd.database.common.FeatureExtractor;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.main.GPDMain;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.userinput.DatabaseInfo;
import edu.umich.gpd.userinput.SampleInfo;
import edu.umich.gpd.util.GPDLogger;
import edu.umich.gpd.util.UtilFunctions;
import edu.umich.gpd.workload.Query;
import edu.umich.gpd.workload.Workload;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2/26/17.
 */
public class GreedySolver extends AbstractSolver {

  public GreedySolver(Connection conn, Workload workload, Schema schema,
                      Set<Configuration> configurations, List<SampleInfo> sampleDBs,
                      DatabaseInfo dbInfo, FeatureExtractor extractor, boolean useRegression) {
    super(conn, workload, schema, configurations, sampleDBs, dbInfo, extractor, useRegression);
  }

  @Override
  public boolean solve() {
    List<Structure> possibleStructures = getAllStructures(configurations);
    this.sizeLimit = GPDMain.userInput.getSetting().getSizeLimit();
    int numStructures = possibleStructures.size();
    int numSampleDBs = sampleDBs.size();

    if (sizeLimit <= 0) {
      System.out.println("Optimal structures:");
      for (int t = 0; t < numStructures; ++t) {
        System.out.println("\t"+possibleStructures.get(t).getQueryString());
      }
      return true;
    }

    List<Query> queries = workload.getQueries();

    // iteratively remove structure with a greedy algorithm
    while (true) {
      for (int d = 0; d < numSampleDBs; ++d) {
        SampleInfo sample = sampleDBs.get(d);
        String dbName = sample.getDbName();
        try {
          conn.setCatalog(dbName);
          // construct all structures first
          GPDLogger.info(this, "constructing every possible structure first.");
          for (Structure s : possibleStructures) {
            s.create(conn);
            extractor.addTrainingDataForSize(dbName, schema, s);
          }
        } catch (SQLException e) {
          GPDLogger.error(this, "A SQLException has been caught.");
          e.printStackTrace();
          return false;
        }
      }
    }
  }
}
