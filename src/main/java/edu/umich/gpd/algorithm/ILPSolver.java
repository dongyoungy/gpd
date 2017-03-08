package edu.umich.gpd.algorithm;

import com.google.common.base.Stopwatch;
import edu.umich.gpd.database.common.Configuration;
import edu.umich.gpd.database.common.FeatureExtractor;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.main.GPDMain;
import edu.umich.gpd.classifier.GPDClassifier;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.userinput.DatabaseInfo;
import edu.umich.gpd.userinput.SampleInfo;
import edu.umich.gpd.util.GPDLogger;
import edu.umich.gpd.workload.Query;
import edu.umich.gpd.workload.Workload;

import scpsolver.problems.*;
import weka.classifiers.functions.GaussianProcesses;
import weka.classifiers.functions.SMOreg;
import weka.core.Instance;
import weka.core.Utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class ILPSolver extends AbstractSolver {

  private double[][] costArray;
  private double[][][] rawCostArray;
  private int numQuery;
  private int numConfiguration;

  public ILPSolver(Connection conn, Workload workload, Schema schema,
                   Set<Configuration> configurations,
                   List<SampleInfo> sampleDBs,
                   DatabaseInfo dbInfo,
                   FeatureExtractor extractor, boolean useRegression) {

    super(conn, workload, schema, configurations, sampleDBs, dbInfo, extractor, useRegression);
    this.rawCostArray = new double[sampleDBs.size()]
        [workload.getQueries().size()][configurations.size()];
    this.costArray = new double[workload.getQueries().size()][configurations.size()];
    this.numQuery = workload.getQueries().size();
    this.numConfiguration = configurations.size();
  }

  public boolean solve() {
    this.sizeLimit = GPDMain.userInput.getSetting().getSizeLimit();
    Stopwatch entireTime = Stopwatch.createStarted();
    // fill the cost array first.
    Stopwatch timetoFillCostArray = Stopwatch.createStarted();
    GPDLogger.info(this, String.format("Filling the cost & size array with " +
        "%d configurations.", configurations.size()));
    if (!fillCostAndSizeArray()) {
      GPDLogger.error(this, "Failed to fill cost & size arrays.");
      return false;
    }
    long timeTaken = timetoFillCostArray.elapsed(TimeUnit.SECONDS);
    GPDLogger.info(this, String.format("took %d seconds to fill" +
        " the cost array.", timeTaken));

    List<Structure> possibleStructures = getAllStructures(configurations);
    int numStructures = possibleStructures.size();
    boolean[][] compatibilityMatrix = new boolean[numStructures][numStructures];
    // TODO: make this function to be implemented as platform-specific.
    buildCompatibilityMatrix(possibleStructures, compatibilityMatrix);

    LPWizard lpw = new LPWizard();
    for (int i = 0; i < numQuery; ++i) {
      for (int j = 0; j < numConfiguration; ++j) {
        String varName = "x_" + i + "_" + j;
        lpw = lpw.plus(varName, costArray[i][j]);
      }
    }
    lpw.setAllVariablesInteger();
    lpw.setMinProblem(true);


    // add constraints
    for (int i = 0; i < numQuery; ++i) {
      LPWizardConstraint c1 = lpw.addConstraint("c_1_" + i, 1, "=");
      for (int j = 0; j < numConfiguration; ++j) {
        String varName = "x_" + i + "_" + j;
        c1 = c1.plus(varName, 1.0);
      }
      c1.setAllVariablesBoolean();
    }

    // add constaints for x_ij <= y_t
    int constraintCount = 0;
    for (int t = 0; t < possibleStructures.size(); ++t) {
      Structure y = possibleStructures.get(t);
      String yVarName = "y_" + t;
      for (int j = 0; j < configurations.size(); ++j) {
        List<Structure> structureSet = configurations.get(j).getStructures();
        for (Structure s : structureSet) {
          if (s.getName().equals(y.getName())) {
           for (int i = 0; i < numQuery; ++i) {
             LPWizardConstraint c = lpw.addConstraint("c_3_" + constraintCount, 0, ">=");
             String xVarName = "x_" + i + "_" + j;
             c = c.plus(xVarName, 1.0).plus(yVarName, -1.0);
             c.setAllVariablesBoolean();
             ++constraintCount;
           }
          }
        }
      }
    }

    // add constraints for compatibility matrix
    constraintCount = 0;
    for (int i = 0; i < numStructures-1; ++i) {
      String var1 = "y_" + i;
      for (int j = i+1; j < numStructures; ++j) {
        String var2 = "y_" + j;
        int val = 0;
        if (compatibilityMatrix[i][j]) {
          // if compatible
          val = 2;
        } else {
          // if NOT compatible
          val = 1;
        }
        LPWizardConstraint c = lpw.addConstraint("c_4_" + constraintCount, val, ">=");
        c = c.plus(var1, 1.0).plus(var2, 1.0);
        c.setAllVariablesBoolean();
        ++constraintCount;
      }
    }

    // if size limit exists, add it as a constraint
    if (sizeLimit > 0) {
      LPWizardConstraint c = lpw.addConstraint("c_size", sizeLimit, ">=");
      // build classifier for structure size regression
      SMOreg smo = new SMOreg();
      try {
        smo.setOptions(Utils.splitOptions("-C 0"));
      } catch (Exception e) {
        GPDLogger.error(this, "Failed to set options for the classifier.");
        e.printStackTrace();
        return false;
      }
      GPDClassifier sr = new GPDClassifier(smo);
      sr.build(extractor.getTrainDataForSize());
      for (int j = 0; j < numStructures; ++j) {
        String var = "y_" + j;
        Structure s = possibleStructures.get(j);
        Instance testInstance = extractor.getTestInstanceForSize(
            dbInfo.getTargetDBName(), schema, s);
        double structureSize = sr.regress(testInstance);
        c = c.plus(var, structureSize);
      }
      c.setAllVariablesBoolean();
    }

    // now solve
    Stopwatch timeToSolve = Stopwatch.createStarted();
    LPSolution solution = lpw.solve();
    GPDLogger.info(this, "Objective Value = " + solution.getObjectiveValue());
    timeTaken = timeToSolve.elapsed(TimeUnit.SECONDS);
    GPDLogger.info(this, String.format("took %d seconds to solve the problem.", timeTaken));
    timeTaken = entireTime.elapsed(TimeUnit.SECONDS);
    GPDLogger.info(this, String.format("took %d seconds for the entire process.",
          timeTaken));
    //for (int i = 0; i < numQuery; ++i) {
      //for (int j = 0; j < numConfiguration; ++j) {
        //String varName = "x_" + i + "_" + j;
        //System.out.println(varName + " = " + solution.getInteger(varName));
      //}
    //}
    //for (int t = 0; t < numStructures; ++t) {
      //String varName = "y_" + t;
      //System.out.println(varName + " = " + solution.getInteger(varName));
    //}
    Set<Structure> optimalStructures = new LinkedHashSet<>();
    for (int t = 0; t < possibleStructures.size(); ++t) {
      String varName = "y_" + t;
      if (solution.getInteger(varName) == 1) {
        optimalStructures.add(possibleStructures.get(t));
      }
    }

    System.out.println("Optimal structures:");
    for (Structure s : optimalStructures) {
      System.out.println("\t"+s.getQueryString());
    }
    return true;
  }

  private void buildCompatibilityMatrix(List<Structure> possibleStructures,
                                        boolean[][] compatibilityMatrix) {
    // initialize.
    for (int i = 0; i < possibleStructures.size(); ++i) {
      for (int j = 0; j < possibleStructures.size(); ++j) {
        compatibilityMatrix[i][j] = true;
      }
    }

//    for (int i = 0; i < possibleStructures.size(); ++i) {
//      Structure s1 = possibleStructures.get(i);
//      for (int j = i + 1; j < possibleStructures.size(); ++j) {
//        Structure s2 = possibleStructures.get(j);
//        if (s1.getTable().getName().equals(s2.getTable().getName())) {
//          compatibilityMatrix[i][j] = false;
//          compatibilityMatrix[j][j] = false;
//        }
//      }
//    }
  }

  private boolean fillCostAndSizeArray() {

    GPDLogger.info(this, String.format(
        "Filling the cost array."));
    Stopwatch stopwatch;
    List<Query> queries = workload.getQueries();

    if (useRegression || sizeLimit > 0) {
      extractor.initialize(sampleDBs, dbInfo.getTargetDBName(), schema);
    }

    // fill cost array from each sample database.
    for (int d = 0; d < numSampleDBs; ++d) {
      String dbName = sampleDBs.get(d).getDbName();
      Statement stmt;
      try {
        conn.setCatalog(dbName);
        stmt = conn.createStatement();
      } catch (SQLException e) {
        GPDLogger.error(this,"A SQLException has been caught.");
        e.printStackTrace();
        return false;
      }

      for (int j = 0; j < configurations.size(); ++j) {
        Configuration config = configurations.get(j);
        List<Structure> configuration = configurations.get(j).getStructures();
        // build structures
        GPDLogger.info(this, String.format(
            "Building structures for configuration #%d out of %d.", j+1, configurations.size()));
        for (Structure s : configuration) {
          s.create(conn);
          if (useRegression || sizeLimit > 0)
            extractor.addTrainingDataForSize(dbName, schema, s);
        }

        GPDLogger.info(this, String.format(
            "Running queries for configuration #%d out of %d.", j+1, configurations.size()));
        for (int i = 0; i < queries.size(); ++i) {
          Query q = queries.get(i);
          stopwatch = Stopwatch.createStarted();

          boolean isTimedOut = false;
          try {
            stmt.setQueryTimeout(GPDMain.userInput.getSetting().getQueryTimeout());
            stmt.execute(q.getContent());

          } catch (SQLException e) {
            GPDLogger.info(this, String.format("Query #%d has been timed out. Assigning " +
                "maximum cost.", i));
            isTimedOut = true;
          }
          double queryTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
          // when query times out, we assign INT_MAX to its cost.
          if (isTimedOut) {
            rawCostArray[d][i][j] = (long) Integer.MAX_VALUE;
          } else {
            rawCostArray[d][i][j] = (long) queryTime;
          }
          if (useRegression)
            extractor.addTrainingData(dbName, schema, q, config.getNonUniqueString(), queryTime);
        }

        // remove structures
        GPDLogger.info(this, String.format(
            "Removing structures for configuration #%d out of %d.", j+1, configurations.size()));
        for (Structure s : configuration) {
          s.drop(conn);
        }
      }
    }

    // build classifier for structure size regression
    SMOreg smo = new SMOreg();
    try {
      smo.setOptions(Utils.splitOptions("-C 0"));
    } catch (Exception e) {
      GPDLogger.error(this, "Failed to set options for the classifier.");
      e.printStackTrace();
      return false;
    }
    GPDClassifier sr = new GPDClassifier(smo);
    if (useRegression) {
      if (!sr.build(extractor.getTrainData())) {
        return false;
      }
    }
    for (int j = 0; j < configurations.size(); ++j) {
      Configuration config = configurations.get(j);
      for (int i = 0; i < queries.size(); ++i) {
        if (useRegression) {
          Query q = queries.get(i);
          Instance testInstance = extractor.getTestInstance(dbInfo.getTargetDBName(),
              schema, q, config.getNonUniqueString());
          costArray[i][j] = sr.regress(testInstance);
        } else {
          long total = 0;
          for (int d = 0; d < numSampleDBs; ++d) {
            total += rawCostArray[d][i][j];
          }
          costArray[i][j] = total / numSampleDBs;
        }
      }
    }

    return true;
  }
}
