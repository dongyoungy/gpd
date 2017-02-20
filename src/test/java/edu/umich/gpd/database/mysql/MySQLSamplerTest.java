package edu.umich.gpd.database.mysql;

import com.esotericsoftware.minlog.Log;
import com.google.common.base.Stopwatch;
import edu.umich.gpd.database.common.Sampler;
import edu.umich.gpd.main.GPDMain;
import edu.umich.gpd.parser.InputDataParser;
import edu.umich.gpd.parser.SchemaParser;
import edu.umich.gpd.parser.WorkloadParser;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.userinput.*;
import edu.umich.gpd.workload.Workload;

import java.io.File;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dong Young Yoon on 2/19/17.
 */
public class MySQLSamplerTest {
  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("USAGE: MySQLSamplerTest <json_file>");
      System.exit(-1);
    }
    String inputPath = args[0];
    GPDMain.userInput = InputDataParser.parse(new File(inputPath));
    InputData inputData = GPDMain.userInput;

    if (inputData == null) {
      Log.error("MySQLSamplerTest", "Failed to parse JSON specification file.");
      System.exit(-1);
    }
    DatabaseInfo dbInfo = inputData.getDatabaseInfo();
    SchemaInfo schemaInfo = inputData.getSchemaInfo();
    if (schemaInfo.getPath() == null) {
      Log.error("MySQLSamplerTest", "'schemaInfo' is missing 'path' in the JSON " +
          "specification file.");
      System.exit(-1);
    }
    WorkloadInfo workloadInfo = inputData.getWorkloadInfo();
    if (workloadInfo.getPath() == null) {
      Log.error("MySQLSamplerTest", "'workloadInfo' is missing 'path' in the JSON " +
          "specification file.");
      System.exit(-1);
    }

    // parse schema
    String schemaDelimiter = "\n";
    if (schemaInfo.getDelimiter() != null) {
      schemaDelimiter = schemaInfo.getDelimiter();
    }
    SchemaParser schemaParser = new SchemaParser(schemaDelimiter);
    Schema schema = schemaParser.parse(new File(schemaInfo.getPath()));

    // parse workload
    String workloadDelimiter = "\n";
    if (workloadInfo.getDelimiter() != null) {
      workloadDelimiter = workloadInfo.getDelimiter();
    }
    WorkloadParser workloadParser = new WorkloadParser(workloadDelimiter);
    Workload workload = workloadParser.parse(new File(workloadInfo.getPath()));

    Connection conn = null;
    Sampler sampler = null;
    if (dbInfo.getType().equalsIgnoreCase("mysql")) {
      MySQLJDBCConnection mysqlConn = new MySQLJDBCConnection();
      sampler = new MySQLSampler(dbInfo.getTargetDBName());
      conn = mysqlConn.getConnection(dbInfo);
    } else {
      Log.error("MySQLSamplerTest", "Unsupported database type.");
      System.exit(-1);
    }

    if (conn == null) {
      Log.error("MySQLSamplerTest", "Failed to obtain a JDBC connection.");
      System.exit(-1);
    } else if (sampler == null) {
      Log.error("MySQLSamplerTest", "Failed to initialize a database sampler.");
      System.exit(-1);
    }

    List<SampleInfo> samples = inputData.getSetting().getSamples();

    if (samples.isEmpty()) {
      Log.error("MySQLSamplerTest", "The number of sample is zero.");
      System.exit(-1);
    }

    Stopwatch watch = Stopwatch.createStarted();
    Log.info("MySQLSamplerTest", "Sampling started.");
    sampler.sample(conn, schema, 1000, samples);
    long timeTaken = watch.elapsed(TimeUnit.SECONDS);
    Log.info("MySQLSamplerTest", "Sampling done. " + timeTaken +
        " seconds elapsed.");
  }
}
