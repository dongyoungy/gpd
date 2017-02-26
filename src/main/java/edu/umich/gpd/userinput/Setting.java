package edu.umich.gpd.userinput;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dong Young Yoon on 2/16/17.
 */
public class Setting {
  private int maxNumColumn;
  private int minRowForSample;
  private long sizeLimit;
  private boolean useSampling;
  private boolean useRegression;
  private List<SampleInfo> samples;
  private String algorithm;

  public Setting() {
    this.maxNumColumn = 10;
    this.minRowForSample = 1000;
    this.sizeLimit = -1;
    this.useSampling = false;
    this.useRegression = false;
    this.algorithm = "ilp";
    this.samples = new ArrayList<>();
  }

  public int getMaxNumColumn() {
    return (maxNumColumn == 0) ? 10 : maxNumColumn;
  }

  public boolean useSampling() {
    return useSampling;
  }

  public int getMinRowForSample() {
    return minRowForSample;
  }

  public List<SampleInfo> getSamples() {
    return samples;
  }

  public void setMaxNumColumn(int maxNumColumn) {
    this.maxNumColumn = maxNumColumn;
  }

  public long getSizeLimit() {
    return sizeLimit;
  }

  public boolean useRegression() {
    return useRegression;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public void setAlgorithm(String algorithm) {
    this.algorithm = algorithm;
  }
}
