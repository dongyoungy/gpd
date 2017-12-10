package edu.umich.gpd.algorithm;

import com.google.common.base.Stopwatch;
import edu.umich.gpd.classifier.GPDClassifier;
import edu.umich.gpd.database.common.Configuration;
import edu.umich.gpd.database.common.FeatureExtractor;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.main.GPDMain;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.userinput.DatabaseInfo;
import edu.umich.gpd.userinput.SampleInfo;
import edu.umich.gpd.util.GPDLogger;
import edu.umich.gpd.workload.Query;
import edu.umich.gpd.workload.Workload;
import weka.classifiers.functions.SMOreg;
import weka.classifiers.trees.M5P;
import weka.core.Instance;
import weka.core.Utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Created by Dong Young Yoon on 12/6/17. */
public class SASolver extends AbstractSolver {
  private GPDClassifier sizeEstimator;
  private GPDClassifier costEstimator;
  Map<String, Long> structureToQueryTimeMap;
  Map<String, Boolean> structureToUseCacheMap;

  public SASolver(
      Connection conn,
      Workload workload,
      Schema schema,
      Set<Configuration> configurations,
      List<SampleInfo> sampleDBs,
      DatabaseInfo dbInfo,
      FeatureExtractor extractor,
      boolean useRegression) {
    super(conn, workload, schema, configurations, sampleDBs, dbInfo, extractor, useRegression);
    structureToQueryTimeMap = new HashMap<>();
    structureToUseCacheMap = new HashMap<>();
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
    // Train size estimator.
    sizeEstimator.build(extractor.getTrainDataForSize());
    return true;
  }

  private long[] getSizeEstimates(Structure[] structures) {
    long[] sizeEstimates = new long[structures.length];
    for (int i = 0; i < structures.length; ++i) {
      Instance testInstance =
          extractor.getTestInstanceForSize(dbInfo.getTargetDBName(), schema, structures[i]);
      sizeEstimates[i] = (long) sizeEstimator.regress(testInstance);
    }
    return sizeEstimates;
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
                "Estimated query time (from cache) = %d (%s)", cachedQueryTime, getStructureCode(isBuilt)));
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
    }

    List<Query> queries = workload.getQueries();
    long totalQueryTime = 0;
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
            extractor.addTrainingData(dbName, schema, q, builtStructures, queryTime);
          } else {
            totalQueryTime += queryTime;
          }
        }

      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    if (useRegression) {
      costEstimator.build(extractor.getTrainData());
      for (Query q : queries) {
        Instance testInstance =
            extractor.getTestInstance(dbInfo.getTargetDBName(), schema, q, builtStructures);
        if (testInstance == null) {
          GPDLogger.error(this, "test instance null.");
        }
        totalQueryTime += (long) costEstimator.regress(testInstance);
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
          < 0.01) {
        GPDLogger.debug(this, String.format("Using cache for %s", code));
        structureToUseCacheMap.put(code, true);
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
      return Math.exp((-10 * (timeDiff / sizeDiff))) / tempRatio;
      //      return Math.exp((-5 * (timeDiff + sizeDiff))) / ((currentTemp / (10 * targetTemp)));
    }
  }

  private void buildOrDropStructure(Structure st, boolean build) {
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

  @Override
  public boolean solve() {
    List<Structure> allStructures = getAllStructures(configurations);
    Set<Structure> possibleStructures = new LinkedHashSet<>();
    List<String> structureStrList = new ArrayList<>();
    sizeEstimator = new GPDClassifier(new SMOreg());
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
    costEstimator = new GPDClassifier(smo);

    // For now, only consider a single size limit.
    long sizeLimit = sizeLimits[0];

    for (Structure s : allStructures) {
      if (possibleStructures.add(s)) {
        structureStrList.add(s.getNonUniqueString());
      }
    }
    extractor.initialize(sampleDBs, dbInfo.getTargetDBName(), schema, structureStrList);

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

    // Get size estimates for all structures
    GPDLogger.info(this, "Getting estimated structure sizes.");
    long[] estimatedStructureSizes = getSizeEstimates(structureArray);
    for (int i = 0; i < structureArray.length; ++i) {
      GPDLogger.debug(
          this,
          String.format(
              "Estimated Structure Size = %d (%s)",
              estimatedStructureSizes[i], structureArray[i].getQueryString()));
    }

    // Calculate initial temperature (i.e., total estimated size)
    long temperature = 0;
    long targetTemperature = sizeLimit;
    for (long sz : estimatedStructureSizes) {
      temperature += sz;
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
      long sizeDiff = estimatedStructureSizes[indexOfStructureToAlter];
      if (!newSolution[indexOfStructureToAlter]) sizeDiff = sizeDiff * -1;
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
