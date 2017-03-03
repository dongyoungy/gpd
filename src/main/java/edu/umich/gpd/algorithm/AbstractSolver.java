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
public abstract class AbstractSolver {

  protected Connection conn;
  protected Workload workload;
  protected Schema schema;
  protected List<List<Structure>> configurations;
  protected List<SampleInfo> sampleDBs;
  protected DatabaseInfo dbInfo;
  protected FeatureExtractor extractor;
  protected int numSampleDBs;
  protected boolean useRegression;
  protected long sizeLimit;

  public AbstractSolver(Connection conn, Workload workload, Schema schema,
                        Set<List<Structure>> configurations, List<SampleInfo> sampleDBs,
                        DatabaseInfo dbInfo, FeatureExtractor extractor, boolean useRegression) {
    this.conn = conn;
    this.workload = workload;
    this.schema = schema;
    this.configurations = new ArrayList<>(configurations);
    this.sampleDBs = sampleDBs;
    this.numSampleDBs = sampleDBs.size();
    this.dbInfo = dbInfo;
    this.extractor = extractor;
    this.useRegression = useRegression;
  }

  public abstract boolean solve();

  protected List<Structure> getAllStructures(List<List<Structure>> configurations) {
    List<Structure> possibleStructures = new ArrayList<>();
    Set<String> structureNameSet = new HashSet<>();
    for (List<Structure> structures : configurations) {
      for (Structure s : structures) {
        if (structureNameSet.add(s.getName())) {
          possibleStructures.add(s);
        }
      }
    }
    return possibleStructures;
  }
}
