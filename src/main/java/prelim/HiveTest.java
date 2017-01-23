package prelim;

import com.google.common.base.Stopwatch;

import java.io.File;
import java.io.PrintWriter;
import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dong Young Yoon on 1/22/17.
 */
public class HiveTest {
  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";

  public static void main(String[] args) throws SQLException
  {

    if (args.length != 4) {
      System.out.println("USAGE: HiveTest <workload> <data_size> <exec_mem> <log_path>";
      System.exit(-1);
    }

    String workload = args[0];
    String dataSize = args[1];
    String execMem = args[2];
    String logPath = args[3];
    String applicationName = workload + "_" + dataSize + "_" + execMem;
    String inputData = String.format("/data_gen/HiBench/Data/%s/Input", dataSize);
    String outputData = String.format("/data_gen/HiBench/Data/%s/Output", dataSize);

    File timeTakenFile = new File(logPath + File.separator + applicationName + ".time");
    File planFile = new File(logPath + File.separator + applicationName + ".plan");

    PrintWriter timeWriter = null, planWriter = null;

    try {
      timeWriter = new PrintWriter(timeTakenFile);
      planWriter = new PrintWriter(planFile);
      Class.forName(driverName);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }

    Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
    Statement stmt = conn.createStatement();

    switch (workload) {
      case "Scan": {
        stmt.execute("DROP TABLE IF EXISTS uservisits");
        stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/uservisits'", inputData));
        stmt.execute("DROP TABLE IF EXISTS uservisits_copy");
        stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits_copy (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/uservisits_copy'", outputData));

        // get plan
        ResultSet planRes = stmt.executeQuery("EXPLAIN EXTENDED SELECT * FROM uservisits");
        while (planRes.next()) {
          planWriter.println(planRes.getString(1));
        }
        // run query
        Stopwatch watch = new Stopwatch();
        watch.start();
        stmt.execute("INSERT OVERWRITE TABLE uservisits_copy SELECT * FROM uservisits");
        watch.stop();

        long timeTaken = watch.elapsed(TimeUnit.SECONDS);
        timeWriter.println(watch.toString());
        break;
      }
      case "Join": {
        stmt.execute("DROP TABLE IF EXISTS rankings");
        stmt.execute(String.format("CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/rankings'", inputData));
        stmt.execute("DROP TABLE IF EXISTS uservisits_copy");
        stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits_copy (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/uservisits'", inputData));
        stmt.execute("DROP TABLE IF EXISTS rankings_uservisits_join");
        stmt.execute(String.format("CREATE EXTERNAL TABLE rankings_uservisits_join ( sourceIP STRING, avgPageRank DOUBLE, totalRevenue DOUBLE ) STORED AS  SEQUENCEFILE LOCATION '%s/rankings_uservisits_join'", outputData));

        // get plan
        ResultSet planRes = stmt.executeQuery("EXPLAIN EXTENDED SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits_copy UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC");
        while (planRes.next()) {
          planWriter.println(planRes.getString(1));
        }

        // run query
        Stopwatch watch = new Stopwatch();
        watch.start();
        stmt.execute("INSERT OVERWRITE TABLE rankings_uservisits_join SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits_copy UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC");
        watch.stop();

        long timeTaken = watch.elapsed(TimeUnit.SECONDS);
        timeWriter.println(watch.toString());
        break;
      }
      case "Aggregation": {
        stmt.execute("DROP TABLE IF EXISTS uservisits");
        stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '%s/uservisits'", inputData));
        stmt.execute("DROP TABLE IF EXISTS uservisits_aggre");
        stmt.execute(String.format("CREATE EXTERNAL TABLE uservisits_aggre ( sourceIP STRING, sumAdRevenue DOUBLE ) STORED AS SEQUENCEFILE LOCATION '%s/uservisits_aggre'", outputData));

        // get plan
        ResultSet planRes = stmt.executeQuery("EXPLAIN EXTENDED SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP");
        while (planRes.next()) {
          planWriter.println(planRes.getString(1));
        }

        // run query
        Stopwatch watch = new Stopwatch();
        watch.start();
        stmt.execute("INSERT OVERWRITE TABLE uservisits_aggre SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP");
        watch.stop();

        long timeTaken = watch.elapsed(TimeUnit.SECONDS);
        timeWriter.println(watch.toString());
        break;
      }
      default: {
        break;
      }
    }

    timeWriter.close();
    planWriter.close();
  }
}
