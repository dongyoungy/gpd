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
import scpsolver.problems.LPSolution;
import scpsolver.problems.LPWizard;
import scpsolver.problems.LPWizardConstraint;
import weka.classifiers.functions.LibLINEAR;
import weka.classifiers.functions.LibSVM;
import weka.classifiers.functions.SMOreg;
import weka.classifiers.trees.M5P;
import weka.core.Instance;
import weka.core.SelectedTag;
import weka.core.Utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class ILPSolver2 extends AbstractSolver {

  private double[] costArray;
  private double[][] rawCostArray;
  private int numQuery;
  private int numCostVariables;
  private Set<String> configStrSet;
  private Set<String> structureStrSet;
  private Map<String, Integer> configStrMap;

  public ILPSolver2(Connection conn, Workload workload, Schema schema,
                    Set<Configuration> configurations,
                    List<SampleInfo> sampleDBs,
                    DatabaseInfo dbInfo,
                    FeatureExtractor extractor, boolean useRegression) {

    super(conn, workload, schema, configurations, sampleDBs, dbInfo, extractor, useRegression);
    this.numCostVariables = workload.getTotalConfigurationCount();
    this.rawCostArray = new double[sampleDBs.size()]
        [numCostVariables];
    this.costArray = new double[numCostVariables];
    this.numQuery = workload.getQueries().size();
    this.configStrSet = new HashSet<>();
    this.structureStrSet = new HashSet<>();
    this.configStrMap = new HashMap<>();
  }

  public boolean solve() {
    this.sizeLimit = GPDMain.userInput.getSetting().getSizeLimit();
    Stopwatch entireTime = Stopwatch.createStarted();

    int count = 0;
    List<Structure> possibleStructures = getAllStructures(configurations);
    for (Configuration c : configurations) {
      configStrSet.add(c.getNonUniqueString());
      configStrMap.put(c.getNonUniqueString(), count++);
    }
    for (Structure s : possibleStructures) {
      structureStrSet.add(s.getNonUniqueString());
    }

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

    int numStructures = possibleStructures.size();
    boolean[][] compatibilityMatrix = new boolean[numStructures][numStructures];
    // TODO: make this function to be implemented as platform-specific.
    buildCompatibilityMatrix(possibleStructures, compatibilityMatrix);

    count = 0;
    LPWizard lpw = new LPWizard();
    for (int i = 0; i < numQuery; ++i) {
      Query q = workload.getQueries().get(i);
      int numConfig = q.getConfigurations().size();
      for (int j = 0; j < numConfig; ++j) {
        String varName = "x_" + i + "_" + j;
        lpw = lpw.plus(varName, costArray[count]);
        ++count;
      }
    }
    lpw.setAllVariablesInteger();
    lpw.setMinProblem(true);


    // add constraints
    for (int i = 0; i < numQuery; ++i) {
      Query q = workload.getQueries().get(i);
      int numConfig = q.getConfigurations().size();
      LPWizardConstraint c1 = lpw.addConstraint("c_1_" + i, 1, "=");
      for (int j = 0; j < numConfig; ++j) {
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
      for (int i = 0; i < numQuery; ++i) {
        Query q = workload.getQueries().get(i);
        int numConfig = q.getConfigurations().size();
        for (int j = 0; j < numConfig; ++j) {
          Configuration config = configurations.get(j);
          for (Structure s : config.getStructures()) {
            if (s.getName().equals(y.getName())) {
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
      M5P m5p = new M5P();
      try {
        smo.setOptions(Utils.splitOptions("-C 1.0 -N 0 " +
            "-I \"weka.classifiers.functions.supportVector.RegSMOImproved " +
            "-T 0.001 -V -P 1.0E-12 -L 0.001 -W 1\" " +
            "-K \"weka.classifiers.functions.supportVector.PolyKernel -E 1.0 -C 0\""));
        m5p.setOptions(Utils.splitOptions("-R -M 1"));
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
        GPDLogger.debug(this, String.format("Estimated Structure Size = %f (%s)",
            structureSize, s.getQueryString()));
        c = c.plus(var, structureSize);
      }
      c.setAllVariablesBoolean();
    }

    // now solve
    Stopwatch timeToSolve = Stopwatch.createStarted();
    LPSolution solution = lpw.solve();
    if (solution == null) {
      GPDLogger.info(this, "No feasible solution found.");
    } else {
      GPDLogger.info(this, "Objective Value = " + solution.getObjectiveValue());
      timeTaken = timeToSolve.elapsed(TimeUnit.SECONDS);
      GPDLogger.info(this, String.format("took %d seconds to solve the problem.", timeTaken));
      timeTaken = entireTime.elapsed(TimeUnit.SECONDS);
      GPDLogger.info(this, String.format("took %d seconds for the entire process.",
          timeTaken));
    }
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
  }

  private boolean fillCostAndSizeArray() {

    GPDLogger.info(this, String.format(
        "Filling the cost array."));
    Stopwatch stopwatch;
    List<Query> queries = workload.getQueries();

    if (useRegression || sizeLimit > 0) {
      extractor.initialize(sampleDBs, dbInfo.getTargetDBName(), schema,
          new ArrayList<>(structureStrSet),
          new ArrayList<>(configStrSet));
    }

    // fill cost array from each sample database.
    for (int d = 0; d < numSampleDBs; ++d) {
      String dbName = sampleDBs.get(d).getDbName();
      Statement stmt;
      try {
        conn.setCatalog(dbName);
        stmt = conn.createStatement();
      } catch (SQLException e) {
        GPDLogger.error(this, "A SQLException has been caught.");
        e.printStackTrace();
        return false;
      }

      int count = 0;
      Set<Structure> trainedSet = new HashSet<>();
      for (int i = 0; i < queries.size(); ++i) {
        Query q = queries.get(i);
        for (Configuration configuration : q.getConfigurations()) {
          // build structures
          GPDLogger.info(this, String.format(
              "Building structures for configuration #%d out of %d.", configuration.getId() + 1,
              numCostVariables));
          for (Structure s : configuration.getStructures()) {
            s.create(conn);
            if (useRegression || sizeLimit > 0) {
              if (trainedSet.add(s)) {
                extractor.addTrainingDataForSize(dbName, schema, s);
              }
            }
          }

          GPDLogger.info(this, String.format(
              "Running queries for configuration #%d out of %d.", configuration.getId() + 1,
              numCostVariables));
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
            rawCostArray[d][count] = (long) Integer.MAX_VALUE;
          }
          else {
            rawCostArray[d][count] = (long) queryTime;
          }
          if (useRegression)
            extractor.addTrainingData(dbName, schema, q,
                configStrMap.get(configuration.getNonUniqueString()).intValue(),
                queryTime);

          // remove structures
          GPDLogger.info(this, String.format(
              "Removing structures for configuration #%d out of %d.", configuration.getId()+1,
              numCostVariables));
          for (Structure s : configuration.getStructures()) {
            s.drop(conn);
          }
          ++count;
        }
      }
    }

    // build classifier for cost regression
    SMOreg smo = new SMOreg();
    LibLINEAR libLINEAR = new LibLINEAR();
    LibSVM libSVM = new LibSVM();
    libSVM.setSVMType(new SelectedTag(LibSVM.SVMTYPE_EPSILON_SVR, LibSVM.TAGS_SVMTYPE));
    libSVM.setCacheSize(4096);
    try {
      smo.setOptions(Utils.splitOptions("-C 1.0 -N 0 " +
          "-I \"weka.classifiers.functions.supportVector.RegSMOImproved " +
          "-T 0.001 -V -P 1.0E-12 -L 0.001 -W 1\" " +
          "-K \"weka.classifiers.functions.supportVector.PolyKernel -E 1.0 -C 0\""));
    } catch (Exception e) {
      GPDLogger.error(this, "Failed to set options for the classifier.");
      e.printStackTrace();
      return false;
    }
    GPDClassifier sr = new GPDClassifier(libSVM);
    if (useRegression) {
      if (!sr.build(extractor.getTrainData())) {
        return false;
      }
    }
    int count = 0;
    for (int i = 0; i < queries.size(); ++i) {
      Query q = queries.get(i);
      for (Configuration config : q.getConfigurations()) {
        if (useRegression) {
          Instance testInstance = extractor.getTestInstance(dbInfo.getTargetDBName(),
              schema, q, configStrMap.get(config.getNonUniqueString()).intValue());
          costArray[count] = sr.regress(testInstance);
        } else {
          long total = 0;
          for (int d = 0; d < numSampleDBs; ++d) {
            total += rawCostArray[d][count];
          }
          costArray[count] = total / numSampleDBs;
        }
        ++count;
      }
    }

    return true;
  }
}
