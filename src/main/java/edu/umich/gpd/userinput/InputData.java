package edu.umich.gpd.userinput;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class InputData {
  private Setting setting;
  private DatabaseInfo databaseInfo;
  private SchemaInfo schemaInfo;
  private WorkloadInfo workloadInfo;

  @Override
  public String toString() {
    String str = "";
    str += "setting:\n";
    str += "\tmaxNumColumn: " + setting.getMaxNumColumn() + "\n";
    str += "\tsamples:";
    for (SampleInfo s : setting.getSamples()) {
      str += "\n\t\t" + s.getDbName() + ", " + s.getRatio();
    }
    str += "\n";
    str += "databaseInfo:\n";
    str += "\ttype: " + databaseInfo.getType() + "\n";
    str += "\thost: " + databaseInfo.getHost() + "\n";
    str += "\tport: " + databaseInfo.getPort() + "\n";
    str += "\ttargetDBName: " + databaseInfo.getTargetDBName() + "\n";
    str += "schemaInfo:\n";
    str += "\tpath: " + schemaInfo.getPath() + "\n";
    str += "\tdelimiter: " + schemaInfo.getDelimiter() + "\n";
    str += "workloadInfo:\n";
    str += "\tpath: " + workloadInfo.getPath() + "\n";
    str += "\tdelimiter: " + workloadInfo.getDelimiter() + "\n";
    return str;
  }

  public DatabaseInfo getDatabaseInfo() {
    if (databaseInfo == null) {
      databaseInfo = new DatabaseInfo();
    }
    return databaseInfo;
  }

  public SchemaInfo getSchemaInfo() {
    return schemaInfo;
  }

  public Setting getSetting() {
    if (setting == null) {
      setting = new Setting();
    }
    return setting;
  }

  public WorkloadInfo getWorkloadInfo() {
    return workloadInfo;
  }
}
