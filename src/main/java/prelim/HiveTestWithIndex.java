package prelim;

import com.google.common.base.Stopwatch;

import java.io.File;
import java.io.PrintWriter;
import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dong Young Yoon on 1/22/17.
 */
public class HiveTestWithIndex
{
  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";

  public static void main(String[] args) throws SQLException
  {

    if (args.length != 4) {
      System.out.println("USAGE: HiveTestWithIndex <workload> <data_size> <index_type> <log_path>");
      System.exit(-1);
    }

    String workload = args[0];
    String dataSize = args[1];
    String indexType =args[2];
    String logPath = args[3];

    if (!indexType.equalsIgnoreCase("bitmap") && !indexType.equalsIgnoreCase("compact")) {
      System.out.println(String.format("Invalid index type: %s, please use 'compact' or 'bitmap'", indexType));
      System.exit(-1);
    }

    String applicationName = workload + "_" + dataSize;
    String inputData = String.format("/data_gen/HiBench/Data/%s/Input", dataSize);
    String outputData = String.format("/data_gen/HiBench/Data/%s/Output", dataSize);

    File timeTakenFile = new File(logPath + File.separator + applicationName + ".time_with_index_" + indexType);
    File planFile = new File(logPath + File.separator + applicationName + ".plan_with_index_" + indexType);
    File buildIndexFile = new File(logPath + File.separator + applicationName + ".build_index_time_" + indexType);

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

    Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
    Statement stmt = conn.createStatement();

    switch (workload) {
      case "Scan": {
        stmt.execute("DROP INDEX IF EXISTS uservisits_index ON uservisits");
        stmt.execute("DROP TABLE IF EXISTS uservisits");
        stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/uservisits'", inputData));
        stmt.execute("DROP TABLE IF EXISTS uservisits_copy");
        stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits_copy (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/uservisits_copy'", outputData));

        // create index
        Stopwatch watch = Stopwatch.createStarted();
        stmt.execute(String.format("CREATE INDEX uservisits_index ON TABLE uservisits (sourceIP, destURL, visitDate, adRevenue, userAgent, countryCode, languageCode, searchWord, duration) AS '%s' WITH DEFERRED REBUILD", indexType));
        stmt.execute("ALTER INDEX uservisits_index ON uservisits REBUILD");
        watch.stop();
        long timeTaken = watch.elapsed(TimeUnit.SECONDS);
        buildIndexWriter.println(timeTaken);

        // get plan
        ResultSet planRes = stmt.executeQuery("EXPLAIN EXTENDED SELECT sourceIP, destURL, visitDate, adRevenue, userAgent, countryCode, languageCode, searchWord, duration FROM default__uservisits_uservisits_index__");
        while (planRes.next()) {
          planWriter.println(planRes.getString(1));
        }
        // run query
        watch = Stopwatch.createStarted();
        stmt.execute("INSERT OVERWRITE TABLE uservisits_copy SELECT sourceIP, destURL, visitDate, adRevenue, userAgent, countryCode, languageCode, searchWord, duration FROM default__uservisits_uservisits_index__");
        watch.stop();

        timeTaken = watch.elapsed(TimeUnit.SECONDS);
        timeWriter.println(timeTaken);
        break;
      }
      case "Join": {
        stmt.execute("DROP INDEX IF EXISTS rankings_index ON rankings");
        stmt.execute("DROP INDEX IF EXISTS uservisits_copy_index ON uservisits_copy");
        stmt.execute("DROP TABLE IF EXISTS rankings");
        stmt.execute(String.format("CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/rankings'", inputData));
        stmt.execute("DROP TABLE IF EXISTS uservisits_copy");
        stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits_copy (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/uservisits'", inputData));
        stmt.execute("DROP TABLE IF EXISTS rankings_uservisits_join");
        stmt.execute(String.format("CREATE EXTERNAL TABLE rankings_uservisits_join ( sourceIP STRING, avgPageRank DOUBLE, totalRevenue DOUBLE ) STORED AS  SEQUENCEFILE LOCATION '%s/rankings_uservisits_join'", outputData));

        // create index
        Stopwatch watch = Stopwatch.createStarted();
        stmt.execute(String.format("CREATE INDEX rankings_index ON TABLE rankings (pageURL, pageRank) AS '%s' WITH DEFERRED REBUILD", indexType));
        stmt.execute(String.format("CREATE INDEX uservisits_copy_index ON TABLE uservisits_copy (sourceIP, destURL, visitDate, adRevenue) AS '%s' WITH DEFERRED REBUILD", indexType));
        stmt.execute("ALTER INDEX rankings_index ON rankings REBUILD");
        stmt.execute("ALTER INDEX uservisits_copy_index ON uservisits_copy REBUILD");
        watch.stop();
        long timeTaken = watch.elapsed(TimeUnit.SECONDS);
        buildIndexWriter.println(timeTaken);

        // get plan
        ResultSet planRes = stmt.executeQuery("EXPLAIN EXTENDED SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM default__rankings_rankings_index__ R JOIN (SELECT sourceIP, destURL, adRevenue FROM default__uservisits_copy_uservisits_copy_index__ UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC");
        while (planRes.next()) {
          planWriter.println(planRes.getString(1));
        }

        // run query
        watch = Stopwatch.createStarted();
        stmt.execute("INSERT OVERWRITE TABLE rankings_uservisits_join SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM default__rankings_rankings_index__ R JOIN (SELECT sourceIP, destURL, adRevenue FROM default__uservisits_copy_uservisits_copy_index__ UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC");
        watch.stop();

        timeTaken = watch.elapsed(TimeUnit.SECONDS);
        timeWriter.println(timeTaken);
        break;
      }
      case "Aggregation": {
        stmt.execute("DROP INDEX IF EXISTS uservisits_index ON uservisits");
        stmt.execute("DROP TABLE IF EXISTS uservisits");
        stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/uservisits'", inputData));
        stmt.execute("DROP TABLE IF EXISTS uservisits_aggre");
        stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits_aggre ( sourceIP STRING, sumAdRevenue DOUBLE ) STORED AS SEQUENCEFILE LOCATION '%s/uservisits_aggre'", outputData));

        // create index
        Stopwatch watch = Stopwatch.createStarted();
        stmt.execute(String.format("CREATE INDEX uservisits_index ON TABLE uservisits (sourceIP, adRevenue) AS '%s' WITH DEFERRED REBUILD", indexType));
        stmt.execute("ALTER INDEX uservisits_index ON uservisits REBUILD");
        watch.stop();
        long timeTaken = watch.elapsed(TimeUnit.SECONDS);
        buildIndexWriter.println(timeTaken);

        // get plan
        ResultSet planRes = stmt.executeQuery("EXPLAIN EXTENDED SELECT sourceIP, SUM(adRevenue) FROM default__uservisits_uservisits_index__ GROUP BY sourceIP");
        while (planRes.next()) {
          planWriter.println(planRes.getString(1));
        }

        // run query
        watch = Stopwatch.createStarted();
        stmt.execute("INSERT OVERWRITE TABLE uservisits_aggre SELECT sourceIP, SUM(adRevenue) FROM default__uservisits_uservisits_index__ GROUP BY sourceIP");
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
