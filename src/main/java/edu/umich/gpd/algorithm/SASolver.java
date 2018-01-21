package edu.umich.gpd.algorithm;

import com.google.common.base.Stopwatch;
import edu.umich.gpd.classifier.GPDClassifier;
import edu.umich.gpd.database.common.Configuration;
import edu.umich.gpd.database.common.FeatureExtractor;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.database.hive.HiveBooleanParameter;
import edu.umich.gpd.database.hive.HiveNumericParameter;
import edu.umich.gpd.database.hive.HiveParameter;
import edu.umich.gpd.database.mysql.MySQLFeatureExtractor;
import edu.umich.gpd.main.GPDMain;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.userinput.DatabaseInfo;
import edu.umich.gpd.userinput.SampleInfo;
import edu.umich.gpd.util.GPDLogger;
import edu.umich.gpd.workload.Query;
import edu.umich.gpd.workload.Workload;
import org.apache.avro.generic.GenericData;
import weka.classifiers.AbstractClassifier;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.classifiers.functions.SMOreg;
import weka.classifiers.trees.M5P;
import weka.core.Instance;
import weka.core.Utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Created by Dong Young Yoon on 12/6/17. */
public class SASolver extends AbstractSolver {

  SampleInfo sizeSample;
  boolean useActualSize;
  boolean useActualQueryTime;
  String actualDBName;
  private GPDClassifier sizeEstimator;
  private GPDClassifier costEstimator;
  Map<String, Long> structureToQueryTimeMap;
  Map<String, Boolean> structureToUseCacheMap;

  Map<String, Long> parameterToQueryTimeMap;
  Map<String, Boolean> parameterToUseCacheMap;

  Map<Query, FeatureExtractor> queryToExtractorMap;
  Set<Structure> possibleStructures;
  List<String> structureStrList;

  public SASolver(
      Connection conn,
      Workload workload,
      Schema schema,
      Set<Configuration> configurations,
      Set<Structure> structures,
      List<SampleInfo> sampleDBs,
      DatabaseInfo dbInfo,
      FeatureExtractor extractor,
      boolean useRegression) {
    super(
        conn,
        workload,
        schema,
        configurations,
        structures,
        sampleDBs,
        dbInfo,
        extractor,
        useRegression);
    structureToQueryTimeMap = new HashMap<>();
    structureToUseCacheMap = new HashMap<>();
    parameterToUseCacheMap = new HashMap<>();
    parameterToQueryTimeMap = new HashMap<>();
    queryToExtractorMap = new HashMap<>();
    sizeSample = GPDMain.userInput.getSetting().getSampleForSizeCheck();
    useActualSize = GPDMain.userInput.getSetting().useActualSize();
    useActualQueryTime = GPDMain.userInput.getSetting().useActualQueryTime();
    actualDBName = dbInfo.getTargetDBName(); // TODO: temp fix
  }

  private boolean buildInitialFullStructure(Set<Structure> structureSet) {
    for (SampleInfo s : sampleDBs) {
      String dbName = s.getDbName();
      Statement stmt;
      try {
        conn.setCatalog(dbName);
        stmt = conn.createStatement();

        for (Structure st : structureSet) {
          st.create(conn, dbName);
          extractor.addTrainingDataForSize(dbName, schema, st);
        }
      } catch (SQLException e) {
        e.printStackTrace();
        return false;
      }
    }

    // build full structure on actual DB if option is on.
    if (useActualSize || useActualQueryTime) {
      Statement stmt;
      try {
        conn.setCatalog(actualDBName);
        stmt = conn.createStatement();

        for (Structure st : structureSet) {
          st.create(conn, actualDBName);
        }
      } catch (SQLException e) {
        e.printStackTrace();
        return false;
      }
    }

    // create structure for size sample as well.
    //    String dbName = sizeSample.getDbName();
    //    Statement stmt;
    //    try {
    //      conn.setCatalog(dbName);
    //      stmt = conn.createStatement();
    //
    //      for (Structure st : structureSet) {
    //        st.create(conn, dbName);
    //      }
    //    } catch (SQLException e) {
    //      e.printStackTrace();
    //      return false;
    //    }

    return true;
  }

  private long[] getSizeEstimates(String dbName, Structure[] structures, GPDClassifier estimator) {
    // Train size estimator.
    estimator.build(extractor.getTrainDataForSize());

    long[] sizeEstimates = new long[structures.length];
    for (int i = 0; i < structures.length; ++i) {
      Instance testInstance = extractor.getTestInstanceForSize(dbName, schema, structures[i]);
      sizeEstimates[i] = (long) estimator.regress(testInstance);
    }
    return sizeEstimates;
  }

  private void applyHiveParameters(Statement stmt, List<HiveParameter> params) throws SQLException {
    for (HiveParameter param : params) {
      param.apply(stmt);
    }
  }

  private long getTotalQueryTime(List<HiveParameter> parameters) {
    String code = getParameterCode(parameters);
    if (parameterToUseCacheMap.containsKey(code) && parameterToUseCacheMap.get(code)) {
      long cachedQueryTime = getParameterQueryTime(parameters);
      if (cachedQueryTime != -1) {
        GPDLogger.debug(
            this,
            String.format(
                "Estimated query time (from cache) = %d (%s)",
                cachedQueryTime, getParameterCode(parameters)));
        return cachedQueryTime;
      }
    }
    List<Query> queries = workload.getQueries();
    long totalQueryTime = 0;
    if (useActualQueryTime) {
      Statement stmt;
      try {
        conn.setCatalog(actualDBName);
        stmt = conn.createStatement();

        for (Query q : queries) {
          long queryTime = 0;
          boolean timeout = false;
          Stopwatch stopwatch = Stopwatch.createStarted();
          try {
            if (GPDMain.userInput.getDatabaseInfo().getType().equalsIgnoreCase("hive")) {
              stmt.execute("USE " + actualDBName);
              applyHiveParameters(stmt, parameters);
            }
            stmt.execute(q.getContent());
          } catch (SQLException e) {
            GPDLogger.debug(this, String.format("Query #%d has been timed out.", q.getId()));
            timeout = true;
          }
          if (timeout) {
            queryTime = GPDMain.userInput.getSetting().getQueryTimeout() * 1000;
          } else {
            queryTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
          }
          totalQueryTime += queryTime;
        }

      } catch (SQLException e) {
        e.printStackTrace();
      }
    } else {
      for (SampleInfo s : sampleDBs) {
        String dbName = s.getDbName();
        Statement stmt;
        try {
          conn.setCatalog(dbName);
          stmt = conn.createStatement();

          for (Query q : queries) {
            long queryTime = 0;
            boolean timeout = false;
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
              if (GPDMain.userInput.getDatabaseInfo().getType().equalsIgnoreCase("hive")) {
                stmt.execute("USE " + dbName);
                applyHiveParameters(stmt, parameters);
              }
              stmt.execute(q.getContent());
            } catch (SQLException e) {
              GPDLogger.debug(this, String.format("Query #%d has been timed out.", q.getId()));
              timeout = true;
            }
            if (timeout) {
              queryTime = GPDMain.userInput.getSetting().getQueryTimeout() * 1000;
            } else {
              queryTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            }
            totalQueryTime += queryTime;
          }

        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
    GPDLogger.debug(
        this,
        String.format(
            "Estimated query time = %d (%s)", totalQueryTime, getParameterCode(parameters)));
    addParameterQueryTime(parameters, totalQueryTime);
    return totalQueryTime;
  }

  private long getTotalQueryTime(Structure[] structureArray, boolean[] isBuilt) {

    //    if (!useRegression) {
    //      long cachedQueryTime = getStructureQueryTime(isBuilt);
    //      if (cachedQueryTime != -1) {
    //        GPDLogger.debug(
    //            this,
    //            String.format(
    //                "Estimated query time (from cache) = %d (%s)",
    //                cachedQueryTime, getStructureCode(isBuilt)));
    //        return cachedQueryTime;
    //      }
    //    } else {
    //      String code = getStructureCode(isBuilt);
    //      if (structureToUseCacheMap.containsKey(code) && structureToUseCacheMap.get(code)) {
    //        long cachedQueryTime = getStructureQueryTime(isBuilt);
    //        if (cachedQueryTime != -1) {
    //          GPDLogger.debug(
    //              this,
    //              String.format(
    //                  "Estimated query time = %d (%s)", cachedQueryTime,
    // getStructureCode(isBuilt)));
    //          return cachedQueryTime;
    //        }
    //      }
    //    }

    String code = getStructureCode(isBuilt);
    if (structureToUseCacheMap.containsKey(code) && structureToUseCacheMap.get(code)) {
      long cachedQueryTime = getStructureQueryTime(isBuilt);
      if (cachedQueryTime != -1) {
        GPDLogger.debug(
            this,
            String.format(
                "Estimated query time (from cache) = %d (%s)",
                cachedQueryTime, getStructureCode(isBuilt)));
        return cachedQueryTime;
      }
    }
    List<String> builtStructures = new ArrayList<>();
    if (useRegression) {
      for (int i = 0; i < isBuilt.length; ++i) {
        if (isBuilt[i]) {
          builtStructures.add(structureArray[i].getNonUniqueString());
        }
      }
      extractor.clearTrainData();
    }

    List<Query> queries = workload.getQueries();
    long totalQueryTime = 0;
    if (useActualQueryTime) {
      Statement stmt;
      try {
        conn.setCatalog(actualDBName);
        stmt = conn.createStatement();

        for (Query q : queries) {
          long queryTime = 0;
          boolean timeout = false;
          Stopwatch stopwatch = Stopwatch.createStarted();
          try {
            if (GPDMain.userInput.getDatabaseInfo().getType().equalsIgnoreCase("hive")) {
              stmt.execute("USE " + actualDBName);
            }
            stmt.setQueryTimeout(GPDMain.userInput.getSetting().getQueryTimeout());
            stmt.execute(q.getContent());
          } catch (SQLException e) {
            GPDLogger.debug(this, String.format("Query #%d has been timed out.", q.getId()));
            timeout = true;
          }
          if (timeout) {
            queryTime = GPDMain.userInput.getSetting().getQueryTimeout() * 1000;
          } else {
            queryTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
          }
          if (useRegression) {
            // temp
            if (!queryToExtractorMap.containsKey(q)) {
              queryToExtractorMap.put(q, new MySQLFeatureExtractor(conn));
              queryToExtractorMap
                  .get(q)
                  .initialize(
                      sampleDBs,
                      dbInfo.getTargetDBName(),
                      schema,
                      possibleStructures,
                      structureStrList);
            }
            queryToExtractorMap
                .get(q)
                .addTrainingData(actualDBName, schema, q, builtStructures, queryTime);
          } else {
            totalQueryTime += queryTime;
          }
        }

      } catch (SQLException e) {
        e.printStackTrace();
      }
    } else {
      for (SampleInfo s : sampleDBs) {
        String dbName = s.getDbName();
        Statement stmt;
        try {
          conn.setCatalog(dbName);
          stmt = conn.createStatement();

          for (Query q : queries) {
            long queryTime = 0;
            boolean timeout = false;
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
              if (GPDMain.userInput.getDatabaseInfo().getType().equalsIgnoreCase("hive")) {
                stmt.execute("USE " + dbName);
              }
              stmt.setQueryTimeout(GPDMain.userInput.getSetting().getQueryTimeout());
              stmt.execute(q.getContent());
            } catch (SQLException e) {
              GPDLogger.debug(this, String.format("Query #%d has been timed out.", q.getId()));
              timeout = true;
            }
            if (timeout) {
              queryTime = GPDMain.userInput.getSetting().getQueryTimeout() * 1000;
            } else {
              queryTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            }
            if (useRegression) {
              // temp
              if (!queryToExtractorMap.containsKey(q)) {
                queryToExtractorMap.put(q, new MySQLFeatureExtractor(conn));
                queryToExtractorMap
                    .get(q)
                    .initialize(
                        sampleDBs,
                        dbInfo.getTargetDBName(),
                        schema,
                        possibleStructures,
                        structureStrList);
              }
              queryToExtractorMap
                  .get(q)
                  .addTrainingData(dbName, schema, q, builtStructures, queryTime);
            } else {
              totalQueryTime += queryTime;
            }
          }

        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (useRegression) {
        for (Query q : queries) {
          costEstimator.build(queryToExtractorMap.get(q).getTrainData());
          Instance testInstance =
              queryToExtractorMap
                  .get(q)
                  .getTestInstance(dbInfo.getTargetDBName(), schema, q, builtStructures);
          if (testInstance == null) {
            GPDLogger.error(this, "test instance null.");
          }
          totalQueryTime += (long) costEstimator.regress(testInstance);
        }
      }
    }
    GPDLogger.debug(
        this,
        String.format("Estimated query time = %d (%s)", totalQueryTime, getStructureCode(isBuilt)));
    AddStructureQueryTime(isBuilt, totalQueryTime);
    return totalQueryTime;
  }

  private String getStructureCode(boolean[] isBuilt) {
    String code = "";
    for (boolean b : isBuilt) {
      code += (b ? "1" : "0");
    }
    return code;
  }

  private String getParameterCode(List<HiveParameter> params) {
    String code = "";
    for (HiveParameter param : params) {
      code += param.getCode();
    }
    return code;
  }

  private void AddStructureQueryTime(boolean[] isBuilt, long time) {
    String code = getStructureCode(isBuilt);
    Long previousTime = structureToQueryTimeMap.put(code, time);
    if (previousTime == null) {
      structureToUseCacheMap.put(code, false);
    } else {
      GPDLogger.debug(
          this,
          String.format(
              "Previous time = %d, current time = %d (%s)", previousTime.longValue(), time, code));
      if (Math.abs((double) (previousTime.longValue() - time) / (double) previousTime.longValue())
          < 0.001) {
        GPDLogger.debug(this, String.format("Using cache for %s", code));
        structureToUseCacheMap.put(code, true);
      }
    }
  }

  private long getParameterQueryTime(List<HiveParameter> params) {
    String code = getParameterCode(params);
    if (parameterToQueryTimeMap.containsKey(code)) {
      return parameterToQueryTimeMap.get(code);
    }
    return -1;
  }

  private void addParameterQueryTime(List<HiveParameter> params, long time) {
    String code = getParameterCode(params);
    Long previousTime = parameterToQueryTimeMap.put(code, time);
    if (previousTime == null) {
      parameterToUseCacheMap.put(code, false);
    } else {
      GPDLogger.debug(
          this,
          String.format(
              "Previous time = %d, current time = %d (%s)", previousTime.longValue(), time, code));
      if (Math.abs((double) (previousTime.longValue() - time) / (double) previousTime.longValue())
          < 0.001) {
        GPDLogger.debug(this, String.format("Using cache for %s", code));
        parameterToUseCacheMap.put(code, true);
      }
    }
  }

  private long getStructureQueryTime(boolean[] isBuilt) {
    String code = getStructureCode(isBuilt);
    if (structureToQueryTimeMap.containsKey(code)) {
      return structureToQueryTimeMap.get(code);
    }
    return -1;
  }

  private double getAcceptanceProbabilityForParam(
      double timeDiff, double startTemp, double currentTemp, double targetTemp) {
    double tempRatio = currentTemp / startTemp;
    GPDLogger.debug(
        this, String.format("Temp. Ratio = %f (%f, %f)", tempRatio, currentTemp, startTemp));
    if (timeDiff <= 0) return 1.0;
    else {
      return Math.exp(Math.abs(timeDiff) * -1 / (tempRatio / 10));
    }
  }

  private double getAcceptanceProbability(
      double sizeDiff, double timeDiff, double currentTemp, double targetTemp) {
    double tempRatio = currentTemp / targetTemp;
    GPDLogger.debug(
        this, String.format("Temp. Ratio = %f (%f, %f)", tempRatio, currentTemp, targetTemp));
    if (sizeDiff <= 0 && timeDiff <= 0) return 1.0;
    else if (sizeDiff < 0 && timeDiff > 0) {
      return Math.exp(Math.abs(timeDiff / sizeDiff) * -1 / (2 * tempRatio));
    } else if (timeDiff < 0 && sizeDiff > 0) {
      return Math.exp(Math.abs(sizeDiff / timeDiff) * -1 / (2 * tempRatio));
      //      return Math.exp((sizeDiff * timeDiff)) / (currentTemp / (10 * targetTemp));
      //      if (sizeDiff < 0) {
      //        return Math.exp((sizeDiff - (2 * timeDiff)) / (currentTemp / (10 * targetTemp)));
      //      } else {
      //        return Math.exp((timeDiff - (2 * sizeDiff)) / (currentTemp / (10 * targetTemp)));
      //      }
    } else {
      return Math.exp((-5 * (timeDiff / sizeDiff))) / tempRatio;
      //      return Math.exp((-5 * (timeDiff + sizeDiff))) / ((currentTemp / (10 * targetTemp)));
    }
  }

  private void buildOrDropStructure(Structure st, boolean build) {
    if (useActualQueryTime) {
      String dbName = actualDBName;
      Statement stmt;
      try {
        conn.setCatalog(dbName);
        stmt = conn.createStatement();
        if (build) st.create(conn, dbName);
        else st.drop(conn, dbName);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    } else {
      if (useActualSize) {
        String dbName = actualDBName;
        Statement stmt;
        try {
          conn.setCatalog(dbName);
          stmt = conn.createStatement();
          if (build) st.create(conn, dbName);
          else st.drop(conn, dbName);
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      for (SampleInfo s : sampleDBs) {
        String dbName = s.getDbName();
        Statement stmt;
        try {
          conn.setCatalog(dbName);
          stmt = conn.createStatement();
          if (build) st.create(conn, dbName);
          else st.drop(conn, dbName);
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private long getCurrentSize() {
    String dbName = actualDBName;
    Statement stmt;
    try {
      conn.setCatalog(dbName);
      stmt = conn.createStatement();
      ResultSet res =
          stmt.executeQuery(
              String.format(
                  "SELECT SUM(stat_value*@@innodb_page_size) FROM "
                      + "mysql.innodb_index_stats WHERE stat_name = 'size' and database_name = '%s'",
                  dbName));
      if (res.next()) {
        return res.getLong(1);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return 0;
  }

  private void performParameterTuningForHive() {
    List<HiveParameter> hiveParameters = new ArrayList<>();
    // add hive params
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.skewjoin", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.bucketmapjoin", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.bucketmapjoin.sortedmerge", true));
    hiveParameters.add(new HiveBooleanParameter("hive.auto.convert.join", true));
    hiveParameters.add(new HiveBooleanParameter("hive.auto.convert.join.noconditionaltask", true));
    hiveParameters.add(new HiveBooleanParameter("hive.exec.compress.output", true));
    hiveParameters.add(new HiveBooleanParameter("hive.exec.compress.intermediate", true));
    hiveParameters.add(new HiveBooleanParameter("hive.vectorized.execution.enabled", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.index.filter", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.ppd", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.ppd.windowing", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.partition.columns.separate", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.constant.propagation", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.null.scan", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.groupby", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.reducededuplication", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.sort.dynamic.partition", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.sampling.orderby", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.distinct.rewrite", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.union.remove", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.correlation", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.limittranspose", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.filter.stats.reduction", true));
    hiveParameters.add(new HiveBooleanParameter("hive.optimize.skewjoin.compiletime", true));

    hiveParameters.add(
        new HiveNumericParameter(
            "hive.skewjoin.key", 100000, new Integer[] {100, 1000, 10000, 100000}));
    hiveParameters.add(
        new HiveNumericParameter(
            "hive.auto.convert.join.noconditionaltask.size",
            10000,
            new Integer[] {100, 1000, 10000, 100000}));
    hiveParameters.add(
        new HiveNumericParameter(
            "hive.optimize.sampling.orderby.number",
            1000,
            new Integer[] {100, 1000, 10000, 100000}));

    long temperature = GPDMain.userInput.getSetting().getMaxParameterTuningTime();
    long startTemp = temperature;
    long targetTemperature = 1;
    int paramSize = hiveParameters.size();

    GPDLogger.info(this, "Starting parameter tuning for Hive...");
    GPDLogger.info(this, "Initial temperature = " + temperature);
    GPDLogger.info(this, "Target temperature = " + targetTemperature);
    Random rng = new Random();
    GPDLogger.debug(this, "Initial solution = " + getParameterCode(hiveParameters));
    int oldNumericValue = 0;
    boolean oldBooleanValue = false;
    long currentTime = -1;
    int numIteration = 1;
    int lastIndex = -1;
    long bestTime = Long.MAX_VALUE;

    while (temperature >= targetTemperature) {
      int indexOfParamToChange = rng.nextInt(paramSize);
      while (lastIndex != -1 && lastIndex == indexOfParamToChange) {
        indexOfParamToChange = rng.nextInt(paramSize);
      }
      HiveParameter paramToChange = hiveParameters.get(indexOfParamToChange);
      if (paramToChange instanceof HiveBooleanParameter) {
        oldBooleanValue = ((HiveBooleanParameter) paramToChange).getValue();
      } else if (paramToChange instanceof HiveNumericParameter) {
        oldNumericValue = ((HiveNumericParameter) paramToChange).getValue();
      } else {
        GPDLogger.error(this, "Unknown Hive parameter.");
        return;
      }

      GPDLogger.debug(
          this,
          String.format("(Iter #%d) Getting total query time for current solution.", numIteration));
      if (currentTime == -1) {
        currentTime = getTotalQueryTime(hiveParameters);
      }
      paramToChange.changeValueRandom();
      GPDLogger.debug(
          this,
          String.format(
              "(Iter #%d) Getting total query time for neighbor solution.", numIteration));
      long newTime = getTotalQueryTime(hiveParameters);

      double normalizedTimeDiff = (double) (newTime - currentTime) / (double) currentTime;

      double acceptanceProb =
          getAcceptanceProbabilityForParam(
              normalizedTimeDiff, startTemp, temperature, targetTemperature);
      double prob = rng.nextDouble();
      GPDLogger.debug(
          this,
          String.format(
              "(Iter #%d) (Normalized) Time diff. = %f",
              numIteration, normalizedTimeDiff));
      GPDLogger.debug(
          this,
          String.format("(Iter #%d) Acceptance probability = %f", numIteration, acceptanceProb));
      GPDLogger.debug(this, String.format("(Iter #%d) Random value = %f", numIteration, prob));
      if (acceptanceProb > prob) {
        GPDLogger.debug(this, String.format("(Iter #%d) New solution accepted.", numIteration));
        currentTime = newTime;
      } else {
        // Revert param if new solution is not accepted.
        if (paramToChange instanceof HiveBooleanParameter) {
         ((HiveBooleanParameter) paramToChange).setValue(oldBooleanValue);
        } else if (paramToChange instanceof HiveNumericParameter) {
          ((HiveNumericParameter) paramToChange).setValue(oldNumericValue);
        } else {
          GPDLogger.error(this, "Unknown Hive parameter.");
          return;
        }
      }
      if (currentTime < bestTime) bestTime = currentTime;
      GPDLogger.debug(
          this,
          String.format(
              "(Iter #%d) Current solution = %s", numIteration, getParameterCode(hiveParameters)));
      GPDLogger.debug(
          this,
          String.format(
              "(Iter #%d) Current temp = %d, target temp = %d",
              numIteration, temperature, targetTemperature));
      --temperature;
      ++numIteration;
      lastIndex = indexOfParamToChange;
    }
    GPDLogger.info(this, "Optimal parameters found:");
    for (HiveParameter param : hiveParameters) {
      System.out.println("\t" + param.toString());
    }
  }

  @Override
  public boolean solve() {

    if (GPDMain.userInput.getSetting().performParameterTuning()
        && GPDMain.userInput.getDatabaseInfo().getType().equalsIgnoreCase("hive")) {
      performParameterTuningForHive();
      return true;
    }

    List<Structure> allStructures = null;
    if (structures == null) {
      allStructures = getAllStructures(configurations);
    } else {
      allStructures = new ArrayList<>(structures);
    }
    Map<String, Integer> tableIndexCount = new HashMap<>();
    possibleStructures = new LinkedHashSet<>();
    structureStrList = new ArrayList<>();
    sizeLimits = GPDMain.userInput.getSetting().getSizeLimits();

    SMOreg smo = new SMOreg();
    try {
      smo.setOptions(
          Utils.splitOptions(
              "-C 1.0 -N 0 "
                  + "-I \"weka.classifiers.functions.supportVector.RegSMOImproved "
                  + "-T 0.001 -V -P 1.0E-12 -L 0.001 -W 1\" "
                  + "-K \"weka.classifiers.functions.supportVector.PolyKernel -E 1.0 -C 0\""));
      //                  + "-K \"weka.classifiers.functions.supportVector.RBFKernel -G 0.01 -C
      // 0\""));
    } catch (Exception e) {
      GPDLogger.error(this, "Failed to set options for the classifier.");
      e.printStackTrace();
    }
    M5P m5p = new M5P();
    MultilayerPerceptron mp = new MultilayerPerceptron();
    m5p.setBuildRegressionTree(true);
    m5p.setUnpruned(false);
    m5p.setUseUnsmoothed(false);
    LinearRegression linear = new LinearRegression();

    Map<String, AbstractClassifier> wekaClassifiers = new HashMap<>();
    wekaClassifiers.put("SMO", smo);
    wekaClassifiers.put("M5P", m5p);
    wekaClassifiers.put("MultiPerceptron", mp);
    wekaClassifiers.put("Linear", linear);

    costEstimator = new GPDClassifier(smo);

    // For now, only consider a single size limit.
    long sizeLimit = sizeLimits[0];

    for (Structure s : allStructures) {
      int count = 0;
      if (!tableIndexCount.containsKey(s.getTable().getName())) {
        tableIndexCount.put(s.getTable().getName(), 1);
        count = 1;
      } else {
        int currentCount = tableIndexCount.get(s.getTable().getName());
        tableIndexCount.put(s.getTable().getName(), currentCount + 1);
        count = currentCount + 1;
      }
      // TODO: this is temp fix to handle 64 max key limits in MySQL.
      if (GPDMain.userInput.getDatabaseInfo().getType().equalsIgnoreCase("mysql")) {
        if (count <= 64) {
          if (possibleStructures.add(s)) {
            structureStrList.add(s.getNonUniqueString());
          }
        }
      } else {
        if (possibleStructures.add(s)) {
          structureStrList.add(s.getNonUniqueString());
        }
      }
    }
    List<SampleInfo> samples = new ArrayList<>(sampleDBs);
    //    samples.add(sizeSample);
    extractor.initialize(
        samples, dbInfo.getTargetDBName(), schema, possibleStructures, structureStrList);

    Structure[] structureArray = possibleStructures.toArray(new Structure[0]);
    int structureSize = structureArray.length;
    boolean[] isStructureBuilt = new boolean[structureSize];
    Arrays.fill(isStructureBuilt, Boolean.TRUE);

    // Build initial configuration with all structures.
    GPDLogger.info(this, "Building all possible structures.");
    if (!buildInitialFullStructure(possibleStructures)) {
      GPDLogger.error(this, "Failed to build the full set of structures.");
      return false;
    }

    // Get actual size from size sample
    //    long[] sizeFromSizeSample = new long[structureSize];
    //    for (int i = 0; i < structureSize; ++i) {
    //      sizeFromSizeSample[i] = structureArray[i].getSize(sizeSample.getDbName());
    //    }

    //    AbstractClassifier bestSizeClassifier = null;
    //    double minError = Double.MAX_VALUE;
    //    for (Map.Entry<String, AbstractClassifier> classifier : wekaClassifiers.entrySet()) {
    //      sizeEstimator = new GPDClassifier(classifier.getValue());
    //      long[] estimatedStructureSizes =
    //          getSizeEstimates(sizeSample.getDbName(), structureArray, sizeEstimator);
    //      double errorSum = 0.0;
    //      for (int i = 0; i < structureSize; ++i) {
    //        errorSum += Math.pow((sizeFromSizeSample[i] - estimatedStructureSizes[i]), 2);
    //      }
    //      double error = Math.sqrt(errorSum / structureSize);
    //      GPDLogger.debug(this, String.format("Size classifier error for %s:",
    // classifier.getKey()));
    //      GPDLogger.debug(this, String.format("error = %f", error));
    //      if (error < minError) {
    //        minError = error;
    //        bestSizeClassifier = classifier.getValue();
    //      }
    //    }
    //    GPDLogger.debug(
    //        this, String.format("Best size classifier = %s", bestSizeClassifier.toString()));

    // Get size estimates for all structures
    long[] estimatedStructureSizes = null;
    if (useActualSize) {
      estimatedStructureSizes = new long[structureSize];
      GPDLogger.info(this, "Getting structure sizes from the actual database.");
      for (int i = 0; i < structureSize; ++i) {
        estimatedStructureSizes[i] = structureArray[i].getSize(actualDBName);
        GPDLogger.debug(
            this,
            String.format(
                "Estimated Structure Size = %d (%s)",
                estimatedStructureSizes[i], structureArray[i].getQueryString()));
      }
    } else {
      GPDLogger.info(this, "Getting estimated structure sizes from the best classifier.");
      sizeEstimator = new GPDClassifier(smo);
      estimatedStructureSizes =
          getSizeEstimates(dbInfo.getTargetDBName(), structureArray, sizeEstimator);
      for (int i = 0; i < structureArray.length; ++i) {
        GPDLogger.debug(
            this,
            String.format(
                "Estimated Structure Size = %d (%s)",
                estimatedStructureSizes[i], structureArray[i].getQueryString()));
      }
    }
    //
    //    GPDLogger.info(this, "Getting estimated structure sizes from all classifiers.");
    //    for (Map.Entry<String, AbstractClassifier> classifier : wekaClassifiers.entrySet()) {
    //      sizeEstimator = new GPDClassifier(classifier.getValue());
    //      estimatedStructureSizes =
    //          getSizeEstimates(dbInfo.getTargetDBName(), structureArray, sizeEstimator);
    //      GPDLogger.debug(this, String.format("Current size classifier = %s",
    // classifier.getKey()));
    //      for (int i = 0; i < structureArray.length; ++i) {
    //        GPDLogger.debug(
    //            this,
    //            String.format(
    //                "Estimated Structure Size = %d (%s)",
    //                estimatedStructureSizes[i], structureArray[i].getQueryString()));
    //      }
    //    }

    // Calculate initial temperature (i.e., total estimated size)
    long temperature = 0;
    long targetTemperature = sizeLimit;
    if (useActualSize) {
      temperature = getCurrentSize();
    } else {
      for (long sz : estimatedStructureSizes) {
        temperature += sz;
      }
    }

    long totalStructureSize = temperature;
    GPDLogger.info(this, "Initial temperature = " + totalStructureSize);
    GPDLogger.info(this, "Target temperature = " + targetTemperature);
    Random rng = new Random();
    boolean[] currentSolution = Arrays.copyOf(isStructureBuilt, structureSize);
    boolean[] newSolution = Arrays.copyOf(isStructureBuilt, structureSize);
    GPDLogger.debug(this, "Initial solution = " + getStructureCode(currentSolution));
    int numIteration = 1;
    long currentTime = 0;
    long bestTime = Long.MAX_VALUE;
    while (temperature > targetTemperature) {
      int indexOfStructureToAlter = rng.nextInt(structureSize);
      newSolution[indexOfStructureToAlter] =
          currentSolution[indexOfStructureToAlter] ? false : true;

      GPDLogger.debug(
          this,
          String.format("(Iter #%d) Getting total query time for current solution.", numIteration));
      currentTime = getTotalQueryTime(structureArray, currentSolution);
      buildOrDropStructure(
          structureArray[indexOfStructureToAlter], newSolution[indexOfStructureToAlter]);
      GPDLogger.debug(
          this,
          String.format(
              "(Iter #%d) Getting total query time for neighbor solution.", numIteration));
      long newTime = getTotalQueryTime(structureArray, newSolution);

      double normalizedTimeDiff = (double) (newTime - currentTime) / (double) currentTime;
      long sizeDiff = 0;
      if (useActualSize) {
        sizeDiff = getCurrentSize() - temperature;
      } else {
        sizeDiff = estimatedStructureSizes[indexOfStructureToAlter];
        if (!newSolution[indexOfStructureToAlter]) sizeDiff = sizeDiff * -1;
      }
      double normalizedSizeDiff = (double) (sizeDiff) / (double) temperature;

      double acceptanceProb =
          getAcceptanceProbability(
              normalizedSizeDiff, normalizedTimeDiff, temperature, targetTemperature);
      double prob = rng.nextDouble();
      GPDLogger.debug(
          this,
          String.format(
              "(Iter #%d) (Normalized) Time diff. = %f, Size diff. = %f",
              numIteration, normalizedTimeDiff, normalizedSizeDiff));
      GPDLogger.debug(
          this,
          String.format("(Iter #%d) Acceptance probability = %f", numIteration, acceptanceProb));
      GPDLogger.debug(this, String.format("(Iter #%d) Random value = %f", numIteration, prob));
      if (acceptanceProb > prob) {
        GPDLogger.debug(this, String.format("(Iter #%d) New solution accepted.", numIteration));
        currentSolution = Arrays.copyOf(newSolution, structureSize);
        temperature += sizeDiff;
        currentTime = newTime;
      } else {
        // Revert structure if new solution is not accepted.
        buildOrDropStructure(
            structureArray[indexOfStructureToAlter], currentSolution[indexOfStructureToAlter]);
      }
      if (currentTime < bestTime) bestTime = currentTime;
      newSolution = Arrays.copyOf(currentSolution, structureSize);
      GPDLogger.debug(
          this,
          String.format(
              "(Iter #%d) Current solution = %s", numIteration, getStructureCode(currentSolution)));
      GPDLogger.debug(
          this,
          String.format(
              "(Iter #%d) Current temp = %d, target temp = %d",
              numIteration, temperature, targetTemperature));
      ++numIteration;
    }

    GPDLogger.info(this, "Best time = " + bestTime);
    GPDLogger.info(this, "Optimal structures with execution time = " + currentTime + ":");
    for (int i = 0; i < structureArray.length; ++i) {
      if (currentSolution[i]) {
        Structure s = structureArray[i];
        System.out.println("\t" + s.getQueryString());
      }
    }

    return true;
  }
}
