package edu.umich.gpd.userinput;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dong Young Yoon on 2/16/17.
 */
public class Setting {
  private int maxNumColumn;
  private int minRowForSample;
  private boolean useSampling;
  private boolean useRegression;
  private List<SampleInfo> samples;

  public Setting() {
    this.maxNumColumn = 10;
    this.minRowForSample = 1000;
    useSampling = false;
    useRegression = false;
    samples = new ArrayList<>();
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

  public boolean useRegression() {
    return useRegression;
  }
}
