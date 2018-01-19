package edu.umich.gpd.database.hive;

import com.google.common.base.Stopwatch;
import edu.umich.gpd.parser.WorkloadParser;
import edu.umich.gpd.userinput.DatabaseInfo;
import edu.umich.gpd.workload.Query;
import edu.umich.gpd.workload.Workload;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Created by Dong Young Yoon on 1/19/18. */
public class HivePerformanceTest {
  public static void main(String[] args) {

    if (args.length != 5) {
      System.out.println(
          "USAGE: HivePerformanceTest <host> <port> <dbname> <workload_file> <param_file>");
      return;
    }
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    String dbName = args[2];
    String workloadFile = args[3];
    String paramFile = args[4];

    DatabaseInfo dbInfo = new DatabaseInfo();
    dbInfo.setHost(host);
    dbInfo.setPort(port);

    File file = new File(workloadFile);
    WorkloadParser parser = new WorkloadParser("@@@");
    Workload w = parser.parse(file);
    file = new File(paramFile);

    List<String> paramStatements = new ArrayList<>();

    try {
      FileReader reader = new FileReader(file);
      BufferedReader br = new BufferedReader(reader);
      String line;
      System.out.println("Params used:");
      while ((line = br.readLine()) != null) {
        paramStatements.add(line);
        System.out.println("\t" + line);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    HiveJDBCConnection hiveConnection = new HiveJDBCConnection();
    Connection conn = hiveConnection.getConnection(dbInfo);

    List<Long> queryTimes = new ArrayList<>();
    long totalQueryTime = 0;
    try {
      Statement stmt = conn.createStatement();
      for (Query q : w.getQueries()) {
        long queryTime = 0;
        stmt.execute("USE " + dbName);
        for (String paramStmt : paramStatements) {
          stmt.execute(paramStmt);
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        stmt.execute(q.getContent());
        queryTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        totalQueryTime += queryTime;
        queryTimes.add(queryTime);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return;
    }
    for (int i = 0; i < queryTimes.size(); ++i) {
      System.out.println(String.format("Query %d = %d ms", (i+1), queryTimes.get(i)));
    }
    System.out.println(String.format("Total Time = %d ms", totalQueryTime));
  }
}
