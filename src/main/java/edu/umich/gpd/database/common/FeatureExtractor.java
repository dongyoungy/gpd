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
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2/20/17.
 */
public abstract class FeatureExtractor {

  protected Connection conn;
  protected Instances trainData;
  protected Instances trainDataForSize;

  protected FeatureExtractor(Connection conn) {
    this.conn = conn;
  }

  public abstract boolean initialize(List<SampleInfo> sampleDBs, String targetDBName, Schema s,
                                     Set<Structure> structureSet,
                                     List<String> structureStrList);

  public abstract boolean addTrainingData(String dbName, Schema s, Query q, int configId,
                                          double queryTime);

  public abstract boolean addTrainingData(String dbName, Schema s, Query q, List<String> structures,
                                          double queryTime);

  public abstract boolean addTrainingDataForSize(String dbName, Schema s, Structure structure);

  public abstract Instance getTestInstance(String dbName, Schema s, Query q, int configId);

  public abstract Instance getTestInstance(String dbName, Schema s, Query q, List<String> structures);

  public abstract Instance getTestInstanceForSize(String dbName, Schema s, Structure structure);

  public abstract void clearTrainData();

  public Instances getTrainData() {
    return trainData;
  }

  public Instances getTrainDataForSize() {
    return trainDataForSize;
  }
}
