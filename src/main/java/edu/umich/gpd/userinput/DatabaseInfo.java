package edu.umich.gpd.userinput;

import java.util.List;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class DatabaseInfo {
  private String type;
  private String host;
  private int port;
  private String targetDBName;
  private String id;
  private String password;
  private List<String> sampleDBName;
  private List<Double> sampleRatio;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getTargetDBName() {
    return targetDBName;
  }

  public void setTargetDBName(String targetDBName) {
    this.targetDBName = targetDBName;
  }

  public List<String> getSampleDBName() {
    return sampleDBName;
  }

  public String getId() {
    return id;
  }

  public void setSampleDBName(List<String> sampleDBName) {
    this.sampleDBName = sampleDBName;
  }

  public List<Double> getSampleRatio() {
    return sampleRatio;
  }

  public void setSampleRatio(List<Double> sampleRatio) {
    this.sampleRatio = sampleRatio;
  }

  public String getPassword() {
    return password;
  }
}

