package edu.umich.gpd.userinput;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dong Young Yoon on 2/16/17.
 */
public class Setting {
  private int maxNumColumn;
  private boolean sampling;
  private List<SampleInfo> samples;

  public Setting() {
    this.maxNumColumn = 10;
    sampling = false;
    samples = new ArrayList<>();
  }

  public int getMaxNumColumn() {
    return (maxNumColumn == 0) ? 10 : maxNumColumn;
  }

  public boolean isSampling() {
    return sampling;
  }

  public List<SampleInfo> getSamples() {
    return samples;
  }
}
