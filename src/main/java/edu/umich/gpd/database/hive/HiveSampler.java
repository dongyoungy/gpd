package edu.umich.gpd.database.hive;

import edu.umich.gpd.database.common.Sampler;
import edu.umich.gpd.main.GPDMain;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.schema.Table;
import edu.umich.gpd.userinput.SampleInfo;
import edu.umich.gpd.util.GPDLogger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Created by Dong Young Yoon on 1/15/18. */
public class HiveSampler extends Sampler {

  public HiveSampler(String originalDBName) {
    super(originalDBName);
  }

  @Override
  public List<SampleInfo> sample(
      Connection conn, Schema schema, long minRow, List<SampleInfo> sampleInfoList) {
    List<SampleInfo> samples = new ArrayList<>();

    if (sampleInfoList.isEmpty()) {
      GPDLogger.error(this, "Empty sampling information.");
      return null;
    }
    try {
      // get row counts for each table
      List<Table> tableList = schema.getTables();
      Map<Table, Long> tableRowCounts = new HashMap<>();
      Statement stmt = conn.createStatement();
      stmt.execute("USE " + originalDBName);

      GPDLogger.info(this, "Getting table row counts from the original DB.");
      for (Table t : tableList) {
        String tableName = t.getName();
        ResultSet res = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName));
        res.next();
        Long count = res.getLong(1);
        tableRowCounts.put(t, count);
        t.addRowCount(originalDBName, count.longValue());
      }

      // create sample DBs for different file types.
      int sampleDBCount = 1;
      for (SampleInfo sampleInfo : sampleInfoList) {
        String sampleDBNamePrefix = sampleInfo.getDbName();
        double sampleRatio = sampleInfo.getRatio();
        if (sampleRatio < 0 || sampleRatio > 1) {
          GPDLogger.error(this, "Sampling ratio must be between 0 and 1.");
          return null;
        }

        for (HiveFileType fileType : HiveFileType.values()) {
          String sampleDBName = sampleDBNamePrefix + "_" + fileType.getString();
          // drop the DB if exists
          stmt.execute(String.format("DROP DATABASE IF EXISTS %s CASCADE", sampleDBName));
          // create sample DB
          stmt.execute(String.format("CREATE DATABASE %s", sampleDBName));

          stmt = conn.createStatement();
          stmt.execute("USE " + sampleDBName);

          // sample tables
          for (Table t : tableList) {
            String tableName = t.getName();
            // drop table if exists
            stmt.execute(String.format("DROP TABLE IF EXISTS %s", tableName));
            // create table
            GPDLogger.debug(this, "Executing: " + t.getCreateStatement());
            stmt.execute(t.getCreateStatement());

            long rowCount = tableRowCounts.get(t);
            if (rowCount <= minRow || sampleRatio == 1) {
              // copy as-is
              stmt.execute(
                  String.format(
                      "INSERT INTO %s SELECT * FROM %s.%s", tableName, originalDBName, tableName));
            } else {
              // copy using sample ratio (approx.)
              stmt.execute(
                  String.format(
                      "INSERT INTO %s SELECT * FROM %s.%s WHERE rand() <= %f",
                      tableName, originalDBName, tableName, sampleRatio));
            }
          }
          samples.add(new SampleInfo(sampleDBName, sampleRatio, fileType));
          ++sampleDBCount;
        }
      }
    } catch (SQLException e) {
      GPDLogger.error(this, "A SQLException has been caught.");
      e.printStackTrace();
      return null;
    }

    // create a copy of original DB with any of 'actual' options are true.
    if (GPDMain.userInput.getSetting().useActualQueryTime()
        || GPDMain.userInput.getSetting().useActualSize()) {
      GPDLogger.info(this, "Creating a copy of the original DB...");
      String actualDBNamePrefix = originalDBName + "_gpd_actual";

      for (HiveFileType fileType : HiveFileType.values()) {
        String actualDBName = actualDBNamePrefix + "_" + fileType.getString();
        try {
          Statement stmt = conn.createStatement();
          // drop the DB if exists
          stmt.execute(String.format("DROP DATABASE IF EXISTS %s CASCADE", actualDBName));
          // create sample DB
          stmt.execute(String.format("CREATE DATABASE %s", actualDBName));

          stmt = conn.createStatement();
          stmt.execute("USE " + actualDBName);

          List<Table> tableList = schema.getTables();
          for (Table t : tableList) {
            String tableName = t.getName();
            // drop table if exists
            stmt.execute(String.format("DROP TABLE IF EXISTS %s", tableName));
            // create table
            stmt.execute(t.getCreateStatement() + " STORED AS ORC");
            stmt.execute(
                String.format(
                    "INSERT INTO %s SELECT * FROM %s.%s", tableName, originalDBName, tableName));
          }
        } catch (SQLException e) {
          if (e.getErrorCode() == 1007) {
            GPDLogger.info(
                this, "A copy of the original DB already exists. Skipping the creation step.");
          } else {
            GPDLogger.error(this, "A SQLException has been caught.");
            e.printStackTrace();
            return null;
          }
        }
        samples.add(new SampleInfo(actualDBName, 1.0, fileType));
      }
      GPDLogger.info(this, "Copies of the original DB has been created.");
    }

    return samples;
  }
}
