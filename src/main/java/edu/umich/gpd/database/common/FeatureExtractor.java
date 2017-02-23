package edu.umich.gpd.database.common;

import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.userinput.SampleInfo;
import edu.umich.gpd.workload.Query;
import edu.umich.gpd.workload.Workload;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dong Young Yoon on 2/20/17.
 */
public abstract class FeatureExtractor {

  protected Connection conn;
  protected Instances trainData;

  protected FeatureExtractor(Connection conn) {
    this.conn = conn;
  }

  public abstract boolean initialize(List<SampleInfo> sampleDBs, String targetDBName, Schema s);

  public abstract boolean addTrainingData(String dbName, Schema s, Query q, int configIndex,
                                          double queryTime);

  public abstract Instance getTestInstance(String dbName, Schema s, Query q, int configIndex);

  public Instances getTrainData() {
    return trainData;
  }
}
