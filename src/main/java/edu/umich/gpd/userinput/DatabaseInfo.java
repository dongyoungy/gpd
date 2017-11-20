package edu.umich.gpd.userinput;

import edu.umich.gpd.database.common.Structure;

import java.util.ArrayList;
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
  private List<StructureInfo> availableStructures;

  public DatabaseInfo() {
    this.availableStructures = new ArrayList<>();
  }

  public List<StructureInfo> getAvailableStructures() {
    return availableStructures;
  }

  public void setAvailableStructures(List<StructureInfo> availableStructures) {
    this.availableStructures = availableStructures;
  }

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

  public String getId() {
    return id;
  }

  public String getPassword() {
    return password;
  }
}

