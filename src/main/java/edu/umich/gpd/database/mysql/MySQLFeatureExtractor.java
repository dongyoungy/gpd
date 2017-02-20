package edu.umich.gpd.database.mysql;

import edu.umich.gpd.database.common.FeatureExtractor;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.workload.Workload;
import weka.core.Attribute;
import weka.core.Instances;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Created by Dong Young Yoon on 2/20/17.
 */
public class MySQLFeatureExtractor extends FeatureExtractor {

  protected MySQLFeatureExtractor(Connection conn, String dbName) {
    super(conn, dbName);
  }

  @Override
  public ArrayList<Attribute> extractFeatures(Schema s) {

    try {
      conn.setCatalog(dbName);
      Statement stmt = conn.createStatement();

    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Instances extractTrainingData(Schema s, Workload w) {
    return null;
  }
}
