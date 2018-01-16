package edu.umich.gpd.main;

import com.esotericsoftware.minlog.Log;
import edu.umich.gpd.algorithm.*;
import edu.umich.gpd.database.common.*;
import edu.umich.gpd.database.hive.HiveEnumerator;
import edu.umich.gpd.database.hive.HiveFeatureExtractor;
import edu.umich.gpd.database.hive.HiveJDBCConnection;
import edu.umich.gpd.database.hive.HiveSampler;
import edu.umich.gpd.database.mysql.MySQLEnumerator;
import edu.umich.gpd.database.mysql.MySQLFeatureExtractor;
import edu.umich.gpd.database.mysql.MySQLJDBCConnection;
import edu.umich.gpd.database.mysql.MySQLSampler;
import edu.umich.gpd.parser.InputDataParser;
import edu.umich.gpd.parser.SchemaParser;
import edu.umich.gpd.parser.WorkloadParser;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.userinput.*;
import edu.umich.gpd.util.GPDLogger;
import edu.umich.gpd.util.UtilFunctions;
import edu.umich.gpd.workload.Workload;
import org.apache.hadoop.fs.FileSystem;

import javax.rmi.CORBA.Util;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Created by Dong Young Yoon on 2/15/17. */
public class GPDMain {
  public static InputData userInput = new InputData();
  public static FileSystem hadoopFS = null;

  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("USAGE: GPDMain <json_spec_file>");
      System.exit(-1);
    }
    Log.set(Log.LEVEL_INFO);

    String inputPath = args[0];
    userInput = InputDataParser.parse(new File(inputPath));
    InputData inputData = userInput;
    if (inputData == null) {
      Log.error("GPDMain", "Failed to parse JSON specification file.");
      System.exit(-1);
    }
    DatabaseInfo dbInfo = inputData.getDatabaseInfo();
    SchemaInfo schemaInfo = inputData.getSchemaInfo();
    if (schemaInfo.getPath() == null) {
      Log.error("GPDMain", "'schemaInfo' is missing 'path' in the JSON " + "specification file.");
      System.exit(-1);
    }
    WorkloadInfo workloadInfo = inputData.getWorkloadInfo();
    if (workloadInfo.getPath() == null) {
      Log.error("GPDMain", "'workloadInfo' is missing 'path' in the JSON " + "specification file.");
      System.exit(-1);
    }

    if (userInput.getSetting().isDebug()) {
      Log.info("GPDMain", "Debug log enabled.");
      Log.set(Log.LEVEL_DEBUG);
    }

    Log.info("GPDMain", userInput.getSetting().toString());
    Log.info("GPDMain", userInput.toString());

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

    if (schema == null) {
      Log.error(
          "GPDMain",
          "Failed to parse the schema file. Please make sure " + "that the schema file exists.");
      System.exit(-1);
    }
    if (workload == null) {
      Log.error(
          "GPDMain",
          "Failed to parse the schema file. Please make sure " + "that the workload file exists.");
      System.exit(-1);
    }

    Connection conn = null;
    StructureEnumerator enumerator = null;
    Sampler sampler = null;
    FeatureExtractor extractor = null;
    if (dbInfo.getType().equalsIgnoreCase("mysql")) {
      MySQLJDBCConnection mysqlConn = new MySQLJDBCConnection();
      conn = mysqlConn.getConnection(dbInfo);
      enumerator = new MySQLEnumerator();
      sampler = new MySQLSampler(dbInfo.getTargetDBName());
      extractor = new MySQLFeatureExtractor(conn);
    } else if (dbInfo.getType().equalsIgnoreCase("hive")) {
      HiveJDBCConnection hiveConn = new HiveJDBCConnection();
      conn = hiveConn.getConnection(dbInfo);
      enumerator = new HiveEnumerator();
      sampler = new HiveSampler(dbInfo.getTargetDBName());
      extractor = new HiveFeatureExtractor(conn);

      // setup hadoop FS
      org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
      if (dbInfo.getHdfsURI().isEmpty()) {
        Log.error("GPDMain", "hdfsURI must be provided for Hive.");
        System.exit(-1);
      }
      String hdfsURI = dbInfo.getHdfsURI();
      hadoopConf.set("fs.defaultFS", dbInfo.getHdfsURI());
      hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
      try {
        hadoopFS = FileSystem.get(URI.create(hdfsURI), hadoopConf);
      } catch (IOException e) {
        Log.error("GPDMain", "IOException thrown while configuring HDFS.");
        e.printStackTrace();
        System.exit(-1);
      }
    } else {
      Log.error("GPDMain", "Unsupported database type.");
      System.exit(-1);
    }

    if (conn == null) {
      Log.error("GPDMain", "Failed to obtain a JDBC connection.");
      System.exit(-1);
    } else if (enumerator == null) {
      Log.error("GPDMain", "Failed to obtain a configuration enumerator.");
      System.exit(-1);
    }

    Log.info("GPDMain", "Enumerating every possible design structures...");
    Set<Configuration> configurations = null;
    Set<Structure> structures = null;

    if (dbInfo.getType().equalsIgnoreCase("mysql")) {
      configurations = enumerator.enumerateStructures(schema, workload);
    } else if (dbInfo.getType().equalsIgnoreCase("hive")) {
      structures = enumerator.getStructures(schema, workload);
    }
    while (configurations == null
        && structures == null
        && inputData.getSetting().getMaxNumColumn() > 1) {
      int newMaxColumn = inputData.getSetting().getMaxNumColumn() - 1;
      Log.info(
          "GPDMain",
          "There are too many interesting columns to consider. "
              + "Reducing the maximum number of columns to consider to: "
              + newMaxColumn);
      inputData.getSetting().setMaxNumColumn(newMaxColumn);
      configurations = enumerator.enumerateStructures(schema, workload);
    }
    if ((configurations == null || configurations.isEmpty()) && structures == null) {
      Log.error("GPDMain", "Empty configurations and also structures.");
      System.exit(-1);
    }
    Log.info("GPDMain", "Enumeration completed.");
    if (configurations != null) {
      Log.info(
          "GPDMain",
          String.format(
              "Total number of interesting design " + "configurations = %d", configurations.size()));
      Log.info(
          "GPDMain",
          String.format(
              "Total number of physical design structures " + "= %d",
              UtilFunctions.getPossibleStructures(configurations).size()));

      Log.info("GPDMain", "Possible design structures:");
      for (Structure s : UtilFunctions.getPossibleStructures(configurations)) {
        System.out.println("\t" + s.getQueryString());
      }
    } else if (structures != null) {
      Log.info(
          "GPDMain",
          String.format(
              "Total number of physical design structures " + "= %d",
              structures.size()));
      Log.info("GPDMain", "Possible design structures:");
      for (Structure s : structures) {
        System.out.println("\t" + s.getQueryString());
      }
    }

    Setting setting = userInput.getSetting();
    String targetDBName = userInput.getDatabaseInfo().getTargetDBName();
    List<SampleInfo> samples = null;
    List<SampleInfo> inputSamples = null;
    if (setting != null) {
      // create sample DBs
      inputSamples = setting.getSamples();
      int minRowForSample = setting.getMinRowForSample();
      boolean useSampling = setting.useSampling();
      boolean useRegression = setting.useRegression();

      if (useSampling) {
        Log.info("GPDMain", "Generating sample databases...");
        samples = sampler.sample(conn, schema, minRowForSample, inputSamples);
        if (samples != null && !samples.isEmpty()) {
          Log.info("GPDMain", "Sampling databases done.");
        } else {
          Log.error("GPDMain", "Sampling databases failed.");
          System.exit(-1);
        }
        List<SampleInfo> sizeSample = new ArrayList<>();
        sizeSample.add(setting.getSampleForSizeCheck());
        Log.info("GPDMain", "Generating sample databases for size check...");
        if (sampler.sample(conn, schema, minRowForSample, sizeSample) != null) {
          Log.info("GPDMain", "Sampling databases for size check done.");
        } else {
          Log.error("GPDMain", "Sampling databases for size check failed.");
          System.exit(-1);
        }
      } else {
        Log.info(
            "GPDMain",
            String.format(
                "Using the target database '%s' for"
                    + " calculating optimal physical design. Regression has been disabled.",
                targetDBName));
        samples = new ArrayList<>();
        SampleInfo aSample = new SampleInfo(targetDBName, 1.0);
        samples.add(aSample);
        useRegression = false;
      }

      String algorithm = setting.getAlgorithm().toLowerCase();
      AbstractSolver solver = null;

      switch (algorithm) {
        case "ilp":
        case "glpk":
          solver =
              new ILPSolver2(
                  conn,
                  workload,
                  schema,
                  configurations,
                  structures,
                  samples,
                  dbInfo,
                  extractor,
                  useRegression);
          break;
        case "gurobi":
          solver =
              new ILPSolverGurobi(
                  conn,
                  workload,
                  schema,
                  configurations,
                  structures,
                  samples,
                  dbInfo,
                  extractor,
                  useRegression);
          break;
        case "greedy":
          solver =
              new GreedySolver(
                  conn,
                  workload,
                  schema,
                  configurations,
                  structures,
                  samples,
                  dbInfo,
                  extractor,
                  useRegression);
          break;
        case "sa":
          solver =
              new SASolver(
                  conn,
                  workload,
                  schema,
                  configurations,
                  structures,
                  samples,
                  dbInfo,
                  extractor,
                  useRegression);
          break;
        default:
          Log.error("GPDMain", "Unsupported algorithm: " + setting.getAlgorithm());
          System.exit(-1);
      }

      if (!solver.solve()) {
        Log.error("GPDMain", "Failed to solve the optimization problem.");
        System.exit(-1);
      }
    } else {
      Log.error("GPDMain", String.format("Setting null."));
      System.exit(-1);
    }
  }
}
