package edu.umich.gpd.database.common;

import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.workload.Workload;
import weka.core.Attribute;
import weka.core.Instances;

import java.sql.Connection;
import java.util.ArrayList;

/**
 * Created by Dong Young Yoon on 2/20/17.
 */
public abstract class FeatureExtractor {

  protected Connection conn;
  protected String dbName;

  protected FeatureExtractor(Connection conn, String dbName) {
    this.conn = conn;
    this.dbName = dbName;
  }

  public abstract ArrayList<Attribute> extractFeatures(Schema s);

  public abstract Instances extractTrainingData(Schema s, Workload w);
}
