package edu.umich.gpd.main;

import com.esotericsoftware.minlog.Log;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.database.common.StructureEnumerator;
import edu.umich.gpd.database.mysql.MySQLEnumerator;
import edu.umich.gpd.database.mysql.MySQLJDBCConnection;
import edu.umich.gpd.lp.ILPSolver;
import edu.umich.gpd.parser.InputDataParser;
import edu.umich.gpd.parser.SchemaParser;
import edu.umich.gpd.parser.WorkloadParser;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.userinput.*;
import edu.umich.gpd.util.GPDLogger;
import edu.umich.gpd.workload.Workload;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class GPDMain {
  public static InputData userInput = new InputData();
  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("USAGE: GPDMain <json_spec_file>");
      System.exit(-1);
    }

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
      Log.error("GPDMain", "'schemaInfo' is missing 'path' in the JSON " +
          "specification file.");
      System.exit(-1);
    }
    WorkloadInfo workloadInfo = inputData.getWorkloadInfo();
    if (workloadInfo.getPath() == null) {
      Log.error("GPDMain", "'workloadInfo' is missing 'path' in the JSON " +
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
    StructureEnumerator enumerator = null;
    if (dbInfo.getType().equalsIgnoreCase("mysql")) {
      MySQLJDBCConnection mysqlConn = new MySQLJDBCConnection();
      conn = mysqlConn.getConnection(dbInfo);
      enumerator = new MySQLEnumerator();
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
    List<Set<Structure>> configurations = enumerator.enumerateStructures(schema, workload);
    while (configurations == null && inputData.getSetting().getMaxNumColumn() > 1) {
      int newMaxColumn = inputData.getSetting().getMaxNumColumn() - 1;
      Log.info("GPDMain", "There are too many interesting columns to consider. " +
          "Reducing the maximum number of columns to consider to: " + newMaxColumn);
      inputData.getSetting().setMaxNumColumn(newMaxColumn);
      configurations = enumerator.enumerateStructures(schema, workload);
    }
    if (configurations == null || configurations.isEmpty()) {
      Log.error("GPDMain", "Empty configurations.");
      System.exit(-1);
    }
    Log.info("GPDMain", "Enumeration completed.");

    Setting setting = userInput.getSetting();
    if (setting != null) {
      String dbName = setting.getSamples().get(0).getDbName();
      try {
        // TODO: support multiple sample DBs
        conn.setCatalog(dbName);
        ILPSolver solver = new ILPSolver(conn, workload, configurations);
        solver.solve();
      } catch (SQLException e) {
        Log.error("GPDMain",
            String.format("Failed to use the database '%s'.", dbName));
        System.exit(-1);
      }
    } else {
      Log.error("GPDMain",
          String.format("Setting null"));
      System.exit(-1);
    }
  }
}
