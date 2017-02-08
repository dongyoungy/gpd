package prelim;

import com.google.common.base.Stopwatch;

import java.io.File;
import java.io.PrintWriter;
import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dong Young Yoon on 1/22/17.
 */
public class MySQLTestWithIndex
{
  private static final String driverName = "com.mysql.jdbc.Driver";

  public static void main(String[] args) throws SQLException
  {

    if (args.length != 3) {
      System.out.println("USAGE: MySQLTestWithIndex <workload> <data_size> <log_path>");
      System.exit(-1);
    }

    String workload = args[0];
    String dataSize = args[1];
    String logPath = args[2];

    String databaseName = "hibench_sql_" + dataSize;
    String applicationName = workload + "_" + dataSize;

    File timeTakenFile = new File(logPath + File.separator + applicationName + ".time_with_index");
    File planFile = new File(logPath + File.separator + applicationName + ".plan_with_index");
    File buildIndexFile = new File(logPath + File.separator + applicationName + ".build_index_time");

    PrintWriter timeWriter = null, planWriter = null, buildIndexWriter = null;

    try {
      timeWriter = new PrintWriter(timeTakenFile);
      planWriter = new PrintWriter(planFile);
      buildIndexWriter = new PrintWriter(buildIndexFile);
      Class.forName(driverName);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }

    Connection conn = DriverManager.getConnection(String.format("jdbc:mysql://localhost:3400/%s?user=root&password=root", databaseName));
    Statement stmt = conn.createStatement();

    switch (workload) {
      case "All": {
        // create index
        stmt.execute("DROP INDEX IF EXISTS uservisits_index ON uservisits");
        Stopwatch watch = Stopwatch.createStarted();
        stmt.execute(String.format("CREATE INDEX uservisits_index ON TABLE uservisits (sourceIP, destURL, visitDate, adRevenue)"));
        watch.stop();
        long timeTaken = watch.elapsed(TimeUnit.SECONDS);
        buildIndexWriter.println(timeTaken);

        // get plan
        planWriter.println("For Scan:");
        ResultSet planRes = stmt.executeQuery("EXPLAIN EXTENDED SELECT sourceIP, destURL, visitDate, adRevenue FROM uservisits");
        ResultSetMetaData rsmd = planRes.getMetaData();
        int columnNumber = rsmd.getColumnCount();
        for (int i = 0; i < columnNumber; ++i) {
          planWriter.print(rsmd.getColumnName(i) + ",");
        }
        planWriter.println();
        while (planRes.next()) {
          for (int i = 0; i < columnNumber; ++i) {
            planWriter.print(planRes.getString(i) + ",");
          }
          planWriter.println();
        }
        planWriter.println("End Scan");
        planWriter.println();

        watch = Stopwatch.createStarted();
        stmt.execute("SELECT sourceIP, destURL, visitDate, adRevenue FROM uservisits");
        watch.stop();

        timeTaken = watch.elapsed(TimeUnit.SECONDS);
        timeWriter.println("Scan: " + timeTaken);

        // get plan
        planRes = stmt.executeQuery("EXPLAIN EXTENDED SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC");
        rsmd = planRes.getMetaData();
        columnNumber = rsmd.getColumnCount();
        planWriter.println("For Join:");
        for (int i = 0; i < columnNumber; ++i) {
          planWriter.print(rsmd.getColumnName(i) + ",");
        }
        planWriter.println();
        while (planRes.next()) {
          for (int i = 0; i < columnNumber; ++i) {
            planWriter.print(planRes.getString(i) + ",");
          }
          planWriter.println();
        }
        planWriter.println("End Join");
        planWriter.println();

        // run query for Join
        watch = Stopwatch.createStarted();
        stmt.execute("SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC");
        watch.stop();

        timeTaken = watch.elapsed(TimeUnit.SECONDS);
        timeWriter.println("Join: " + timeTaken);

        // get plan
        planRes = stmt.executeQuery("EXPLAIN EXTENDED SELECT sourceIP, SUM(adRevenue) FROM default__uservisits_uservisits_index__ GROUP BY sourceIP");
        rsmd = planRes.getMetaData();
        columnNumber = rsmd.getColumnCount();
        planWriter.println("For Aggregation:");
        for (int i = 0; i < columnNumber; ++i) {
          planWriter.print(rsmd.getColumnName(i) + ",");
        }
        planWriter.println();
        while (planRes.next()) {
          for (int i = 0; i < columnNumber; ++i) {
            planWriter.print(planRes.getString(i) + ",");
          }
          planWriter.println();
        }
        planWriter.println("End Aggregation");
        planWriter.println();

        // run query for Aggregation
        watch = Stopwatch.createStarted();
        stmt.execute("SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP");
        watch.stop();

        timeTaken = watch.elapsed(TimeUnit.SECONDS);
        timeWriter.println(timeTaken);
        break;
      }
      case "Scan": {
        stmt.execute("DROP INDEX IF EXISTS uservisits_index ON uservisits");

        // create index
        Stopwatch watch = Stopwatch.createStarted();
        stmt.execute(String.format("CREATE INDEX uservisits_index ON TABLE uservisits (sourceIP, destURL, visitDate, adRevenue)"));
        watch.stop();
        long timeTaken = watch.elapsed(TimeUnit.SECONDS);
        buildIndexWriter.println(timeTaken);

        // get plan
        ResultSet planRes = stmt.executeQuery("EXPLAIN EXTENDED SELECT sourceIP, destURL, visitDate, adRevenue FROM uservisits");
        ResultSetMetaData rsmd = planRes.getMetaData();
        int columnNumber = rsmd.getColumnCount();
        for (int i = 0; i < columnNumber; ++i) {
          planWriter.print(rsmd.getColumnName(i) + ",");
        }
        planWriter.println();
        while (planRes.next()) {
          for (int i = 0; i < columnNumber; ++i) {
            planWriter.print(planRes.getString(i) + ",");
          }
          planWriter.println();
        }

        // run query
        watch = Stopwatch.createStarted();
        stmt.execute("SELECT sourceIP, destURL, visitDate, adRevenue FROM default__uservisits_uservisits_index__");
        watch.stop();

        timeTaken = watch.elapsed(TimeUnit.SECONDS);
        timeWriter.println(timeTaken);
        break;
      }
      case "Join": {
        stmt.execute("DROP INDEX IF EXISTS uservisits_index ON uservisits");

        // create index
        Stopwatch watch = Stopwatch.createStarted();
        stmt.execute(String.format("CREATE INDEX uservisits_index ON TABLE uservisits (sourceIP, destURL, visitDate, adRevenue)"));
        watch.stop();
        long timeTaken = watch.elapsed(TimeUnit.SECONDS);
        buildIndexWriter.println(timeTaken);

        // get plan
        ResultSet planRes = stmt.executeQuery("EXPLAIN EXTENDED SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC");
        ResultSetMetaData rsmd = planRes.getMetaData();
        int columnNumber = rsmd.getColumnCount();
        for (int i = 0; i < columnNumber; ++i) {
          planWriter.print(rsmd.getColumnName(i) + ",");
        }
        planWriter.println();
        while (planRes.next()) {
          for (int i = 0; i < columnNumber; ++i) {
            planWriter.print(planRes.getString(i) + ",");
          }
          planWriter.println();
        }

        // run query
        watch = Stopwatch.createStarted();
        stmt.execute("SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC");
        watch.stop();

        timeTaken = watch.elapsed(TimeUnit.SECONDS);
        timeWriter.println(timeTaken);
        break;
      }
      case "Aggregation": {
        stmt.execute("DROP INDEX IF EXISTS uservisits_index ON uservisits");

        // create index
        Stopwatch watch = Stopwatch.createStarted();
        stmt.execute(String.format("CREATE INDEX uservisits_index ON TABLE uservisits (sourceIP, destURL, visitDate, adRevenue)"));
        watch.stop();
        long timeTaken = watch.elapsed(TimeUnit.SECONDS);
        buildIndexWriter.println(timeTaken);

        // get plan
        ResultSet planRes = stmt.executeQuery("EXPLAIN EXTENDED SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP");
        while (planRes.next()) {
          planWriter.println(planRes.getString(1));
        }

        // run query
        watch = Stopwatch.createStarted();
        stmt.execute("SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP");
        watch.stop();

        timeTaken = watch.elapsed(TimeUnit.SECONDS);
        timeWriter.println(timeTaken);
        break;
      }
      default: {
        break;
      }
    }

    buildIndexWriter.close();
    timeWriter.close();
    planWriter.close();
  }
}
