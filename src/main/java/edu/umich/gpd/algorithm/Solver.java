package edu.umich.gpd.algorithm;

import edu.umich.gpd.database.common.FeatureExtractor;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.userinput.DatabaseInfo;
import edu.umich.gpd.userinput.SampleInfo;
import edu.umich.gpd.workload.Workload;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2/26/17.
 */
public abstract class Solver {

  protected Connection conn;
  protected Workload workload;
  protected Schema schema;
  protected List<Set<Structure>> configurations;
  protected List<SampleInfo> sampleDBs;
  protected DatabaseInfo dbInfo;
  protected FeatureExtractor extractor;
  protected boolean useRegression;
  protected long sizeLimit;

  public Solver(Connection conn, Workload workload, Schema schema,
                List<Set<Structure>> configurations, List<SampleInfo> sampleDBs,
                DatabaseInfo dbInfo, FeatureExtractor extractor, boolean useRegression) {
    this.conn = conn;
    this.workload = workload;
    this.schema = schema;
    this.configurations = configurations;
    this.sampleDBs = sampleDBs;
    this.dbInfo = dbInfo;
    this.extractor = extractor;
    this.useRegression = useRegression;
  }

  public abstract boolean solve();

  protected List<Structure> getPossibleStructures(List<Set<Structure>> configurations) {
    Set<Structure> possibleStructures = new HashSet<>();
    for (Set<Structure> structures : configurations) {
      for (Structure s : structures) {
        possibleStructures.add(s);
      }
    }
    return new ArrayList(possibleStructures);
  }
}
