package edu.umich.gpd.database.common;

import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.userinput.SampleInfo;

import java.sql.Connection;
import java.util.List;

/**
 * Created by Dong Young Yoon on 2/19/17.
 */
public abstract class Sampler {
  protected String originalDBName;

  public Sampler(String originalDBName) {
    this.originalDBName = originalDBName;
  }

  public abstract boolean sample(Connection conn, Schema schema,
                                 long minRow, List<SampleInfo> sampleInfoList);
}
