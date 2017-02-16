package edu.umich.gpd.userinput;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class DatabaseInfo {
  private String type;
  private String host;
  private int port;
  private String dbName;
  private String id;
  private String password;
  private String sampleDBName;

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

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getSampleDBName() {
    return sampleDBName;
  }

  public String getId() {
    return id;
  }

  public String getPassword() {
    return password;
  }
}

