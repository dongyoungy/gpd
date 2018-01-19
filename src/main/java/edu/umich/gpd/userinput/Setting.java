package edu.umich.gpd.userinput;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dong Young Yoon on 2/16/17.
 */
public class Setting {
  private int maxNumColumn;
  private int maxNumColumnPerStructure;
  private int maxNumTablePerQuery;
  private int maxNumStructurePerTable;
  private int minRowForSample;
  private int queryTimeout;
  private long[] sizeLimits;
  private long incrementalRunTime;
  private boolean useSampling;
  private boolean useRegression;
  private boolean debug;
  private boolean incrementalRun;
  private boolean useActualSize;
  private boolean useActualQueryTime;
  private boolean performParameterTuning;
  private int maxParameterTuningTime;
  private int ilpTimeLimit;
  private List<SampleInfo> samples;
  private SampleInfo sampleForSizeCheck;
  private String algorithm;
  private double tolerableLatencyMultiplier;

  public Setting() {
    this.maxNumColumn = 30;
    this.maxNumColumnPerStructure = 2;
    this.maxNumTablePerQuery = 2;
    this.maxNumStructurePerTable = 2;
    this.minRowForSample = 1000;
    this.queryTimeout = 30;
    this.sizeLimits = new long[1];
    this.useSampling = false;
    this.useRegression = false;
    this.incrementalRun = false;
    this.useActualSize = false;
    this.useActualQueryTime = false;
    this.incrementalRunTime = 0;
    this.performParameterTuning = false;
    this.maxParameterTuningTime = 300;
    this.ilpTimeLimit = 1800;
    this.debug = false;
    this.algorithm = "ilp";
    this.samples = new ArrayList<>();
    this.sampleForSizeCheck = new SampleInfo("gpd_size_sample", 0.25);
    this.tolerableLatencyMultiplier = 10.0;
  }

  @Override
  public String toString() {
    String sizeLimitStr = "{";
    for (long sz : sizeLimits) {
      sizeLimitStr += sz;
      sizeLimitStr += ",";
    }
    sizeLimitStr += "}";
    return "Current Setting = {" +
        "maxNumColumn=" + maxNumColumn +
        ", maxNumColumnPerStructure=" + maxNumColumnPerStructure +
        ", maxNumTablePerQuery=" + maxNumTablePerQuery +
        ", maxNumStructurePerTable=" + maxNumStructurePerTable +
        ", minRowForSample=" + minRowForSample +
        ", queryTimeout=" + queryTimeout +
        ", sizeLimits=" + sizeLimitStr +
        ", useSampling=" + useSampling +
        ", useRegression=" + useRegression +
        ", useActualSize=" + useActualSize +
        ", useActualQueryTime=" + useActualQueryTime +
        ", incrementalRun=" + incrementalRun +
        ", incrementalRunTime=" + incrementalRunTime +
        ", performParameterTuning=" + performParameterTuning +
        ", maxParameterTuningTime=" + maxParameterTuningTime +
        ", tolerableLatencyMultipler=" + tolerableLatencyMultiplier +
        ", ilpTimeLimit=" + ilpTimeLimit +
        ", debug=" + debug +
        ", samples=" + samples +
        ", algorithm='" + algorithm + '\'' +
        '}';
  }

  public boolean performParameterTuning() {
    return performParameterTuning;
  }

  public int getMaxParameterTuningTime() {
    return maxParameterTuningTime;
  }

  public void setMaxNumColumnPerStructure(int maxNumColumnPerStructure) {
    this.maxNumColumnPerStructure = maxNumColumnPerStructure;
  }

  public boolean useActualSize() {
    return useActualSize;
  }

  public boolean useActualQueryTime() {
    return useActualQueryTime;
  }

  public void setDebug(boolean debug) {

    this.debug = debug;
  }

  public int getMaxNumColumnPerStructure() {
    return maxNumColumnPerStructure;
  }

  public boolean isDebug() {
    return debug;
  }

  public int getMaxNumColumn() {
    return (maxNumColumn == 0) ? 30 : maxNumColumn;
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

  public long[] getSizeLimits() {
    return sizeLimits;
  }

  public boolean useRegression() {
    return useRegression;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public int getQueryTimeout() {
    return queryTimeout;
  }

  public void setQueryTimeout(int queryTimeout) {
    this.queryTimeout = queryTimeout;
  }

  public void setAlgorithm(String algorithm) {
    this.algorithm = algorithm;
  }

  public boolean isIncrementalRun() {
    return incrementalRun;
  }

  public long getIncrementalRunTime() {
    return incrementalRunTime;
  }

  public void setMaxNumTablePerQuery(int maxNumTablePerQuery) {
    this.maxNumTablePerQuery = maxNumTablePerQuery;
  }

  public void setMaxNumStructurePerTable(int maxNumStructurePerTable) {
    this.maxNumStructurePerTable = maxNumStructurePerTable;
  }

  public void setIncrementalRunTime(long incrementalRunTime) {
    this.incrementalRunTime = incrementalRunTime;
  }

  public int getIlpTimeLimit() {
    return ilpTimeLimit;
  }

  public void setIlpTimeLimit(int ilpTimeLimit) {
    this.ilpTimeLimit = ilpTimeLimit;
  }

  public void setIncrementalRun(boolean incrementalRun) {
    this.incrementalRun = incrementalRun;
  }

  public int getMaxNumTablePerQuery() {
    return maxNumTablePerQuery;
  }

  public int getMaxNumStructurePerTable() {
    return maxNumStructurePerTable;
  }

  public SampleInfo getSampleForSizeCheck() {
    return sampleForSizeCheck;
  }

  public double getTolerableLatencyMultiplier() {
    return tolerableLatencyMultiplier;
  }
}
