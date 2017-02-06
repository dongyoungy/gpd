package prelim;

import com.google.common.base.Stopwatch;

import java.io.File;
import java.io.PrintWriter;
import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dong Young Yoon on 2/3/17.
 */
public class HiveTestWithSample {

  public static void main(String[] args) throws SQLException {

    if (args.length != 4) {
      System.out.println("USAGE: HiveTestWithSample <data_dir> <log_path> <workload> <sample_size>");
      System.exit(-1);
    }

    String dataDir = args[0];
    String logPath = args[1];
    String workload = args[2];
    String sampleSize = args[3];

    String applicationName = workload + "_s" + sampleSize;
    String uservisitsData = String.format("%s/uservisits", dataDir);
    String rankingsData = String.format("%s/rankings", dataDir);
    String outputPath = String.format("%s/Output", dataDir);

    // target sample size = 1m, 50m, 200m, 500m, 1g
//    String[] sampleSizeStr = {"1m", "50m", "200m", "500m", "1g"};
//    String[] workloads = {"Scan", "Join", "Aggregation"};
//    long[] uservisitsSampleSize = {5000, 250000, 1000000, 2500000, 5000000};
//    long[] rankingsSampleSize = {600, 30000, 120000, 300000, 600000};

    File timeTakenFile = new File(logPath + File.separator + applicationName + ".time_with_index");
    File planFile = new File(logPath + File.separator + applicationName + ".plan_with_index");
    File buildIndexFile = new File(logPath + File.separator + applicationName + ".build_index_time");

    PrintWriter timeWriter = null, planWriter = null, buildIndexWriter = null;

    try {
      timeWriter = new PrintWriter(timeTakenFile);
      planWriter = new PrintWriter(planFile);
      buildIndexWriter = new PrintWriter(buildIndexFile);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }

    Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
    Statement stmt = conn.createStatement();

    String uservisitsTable = String.format("uservisits_sample_%s", sampleSize);
    String rankingsTable = String.format("rankings_sample_%s", sampleSize);

    stmt.execute(String.format("DROP INDEX IF EXISTS sample_index ON %s", uservisitsTable));

    // create index
    Stopwatch watch = Stopwatch.createStarted();
    stmt.execute(String.format("CREATE INDEX sample_index ON TABLE %s (sourceIP, destURL, visitDate, adRevenue) AS 'compact' WITH DEFERRED REBUILD", uservisitsTable));
    stmt.execute(String.format("ALTER INDEX sample_index ON %s REBUILD", uservisitsTable));
    watch.stop();
    long timeTaken = watch.elapsed(TimeUnit.SECONDS);
    buildIndexWriter.println(timeTaken);
    String uservisitsIndex = String.format("default__%s_sample_index__", uservisitsTable);

    switch (workload) {
      case "Scan": {
        stmt.execute(String.format("DROP TABLE IF EXISTS uservisits_copy_%s", applicationName));
        stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits_copy_%s (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/uservisits_copy'", applicationName, outputPath));

        // get plan
        ResultSet planRes = stmt.executeQuery(String.format("EXPLAIN SELECT sourceIP, destURL, visitDate, adRevenue FROM %s", uservisitsIndex));
        while (planRes.next()) {
          planWriter.println(planRes.getString(1));
        }
        // run query
        watch = Stopwatch.createStarted();
        stmt.execute(String.format("INSERT OVERWRITE TABLE uservisits_copy_%s SELECT sourceIP, destURL, visitDate, adRevenue FROM %s", applicationName, uservisitsIndex));
        watch.stop();

        timeTaken = watch.elapsed(TimeUnit.SECONDS);
        timeWriter.println(timeTaken);
        break;
      }
      case "Join": {
        stmt.execute(String.format("DROP TABLE IF EXISTS rankings_uservisits_%s", applicationName));
        stmt.execute(String.format("CREATE EXTERNAL TABLE rankings_uservisits_%s ( sourceIP STRING, avgPageRank DOUBLE, totalRevenue DOUBLE ) STORED AS  SEQUENCEFILE LOCATION '%s/rankings_uservisits_join'", applicationName, outputPath));

        // get plan
        ResultSet planRes = stmt.executeQuery(String.format("EXPLAIN SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM %s R JOIN (SELECT sourceIP, destURL, adRevenue FROM %s UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC", rankingsTable, uservisitsIndex));
        while (planRes.next()) {
          planWriter.println(planRes.getString(1));
        }

        // run query
        watch = Stopwatch.createStarted();
        stmt.execute(String.format("INSERT OVERWRITE TABLE rankings_uservisits_%s SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM %s R JOIN (SELECT sourceIP, destURL, adRevenue FROM %s UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC", applicationName, rankingsTable, uservisitsIndex));
        watch.stop();

        timeTaken = watch.elapsed(TimeUnit.SECONDS);
        timeWriter.println(timeTaken);
        break;
      }
      case "Aggregation": {
        stmt.execute(String.format("DROP TABLE IF EXISTS uservisits_aggre_%s", applicationName));
        stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits_aggre_%s ( sourceIP STRING, sumAdRevenue DOUBLE ) STORED AS SEQUENCEFILE LOCATION '%s/uservisits_aggre'", applicationName, outputPath));


        // get plan
        ResultSet planRes = stmt.executeQuery(String.format("EXPLAIN SELECT sourceIP, SUM(adRevenue) FROM %s GROUP BY sourceIP", uservisitsIndex));
        while (planRes.next()) {
          planWriter.println(planRes.getString(1));
        }

        // run query
        watch = Stopwatch.createStarted();
        stmt.execute(String.format("INSERT OVERWRITE TABLE uservisits_aggre_%s SELECT sourceIP, SUM(adRevenue) FROM %s GROUP BY sourceIP", applicationName, uservisitsIndex));
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
