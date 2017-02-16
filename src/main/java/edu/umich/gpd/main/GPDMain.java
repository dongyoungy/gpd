package edu.umich.gpd.main;

import com.esotericsoftware.minlog.Log;
import com.mysql.jdbc.authentication.MysqlClearPasswordPlugin;
import edu.umich.gpd.database.Structure;
import edu.umich.gpd.database.StructureEnumerator;
import edu.umich.gpd.database.mysql.MySQLEnumerator;
import edu.umich.gpd.database.mysql.MySQLJDBCConnection;
import edu.umich.gpd.lp.ILPSolver;
import edu.umich.gpd.parser.InputDataParser;
import edu.umich.gpd.parser.SchemaParser;
import edu.umich.gpd.parser.WorkloadParser;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.userinput.DatabaseInfo;
import edu.umich.gpd.userinput.InputData;
import edu.umich.gpd.userinput.SchemaInfo;
import edu.umich.gpd.userinput.WorkloadInfo;
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
  public static final int MAX_NUM_COLUMN = 10;
  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("USAGE: GPDMain <json_spec_file>");
      System.exit(-1);
    }

    String inputPath = args[0];
    InputData inputData = InputDataParser.parse(new File(inputPath));
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

    List<Set<Structure>> configurations = enumerator.enumerateStructures(schema, workload);
    if (configurations == null || configurations.isEmpty()) {
      Log.error("GPDMain", "Empty configurations.");
      System.exit(-1);
    }

    if (dbInfo.getSampleDBName() != null && !dbInfo.getSampleDBName().isEmpty()) {
      try {
        // TODO: support multiple sample DBs
        conn.setCatalog(dbInfo.getSampleDBName().get(0));
      } catch (SQLException e) {
        Log.error("GPDMain",
            String.format("Failed to use the database '%s'.", dbInfo.getSampleDBName()));
        System.exit(-1);
      }
    }
    ILPSolver solver = new ILPSolver(conn, workload, configurations);
    solver.solve();
  }
}
