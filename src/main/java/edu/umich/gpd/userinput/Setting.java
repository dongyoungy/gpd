package edu.umich.gpd.userinput;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dong Young Yoon on 2/16/17.
 */
public class Setting {
  private int maxNumColumn;
  private int minRowForSample;
  private boolean sampling;
  private List<SampleInfo> samples;

  public Setting() {
    this.maxNumColumn = 10;
    this.minRowForSample = 1000;
    sampling = false;
    samples = new ArrayList<>();
  }

  public int getMaxNumColumn() {
    return (maxNumColumn == 0) ? 10 : maxNumColumn;
  }

  public boolean isSampling() {
    return sampling;
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
}
