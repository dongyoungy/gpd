package edu.umich.gpd.algorithm.data;

/**
 * Created by Dong Young Yoon on 11/28/17.
 */
public class TemporalCostArray {
  private double[] array;
  private long timeTaken;
  private String type;

  public TemporalCostArray(double[] array, long timeTaken, String type) {
    this.array = array;
    this.timeTaken = timeTaken;
    this.type = type;
  }

  public double[] getArray() {
    return array;
  }

  public String getType() {
    return type;
  }

  public long getTimeTaken() {
    return timeTaken;
  }
}
