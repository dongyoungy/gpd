package edu.umich.gpd.algorithm;

import com.google.common.base.Stopwatch;
import edu.umich.gpd.algorithm.data.TemporalCostArray;
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
import gurobi.*;
import scpsolver.lpsolver.LinearProgramSolver;
import scpsolver.lpsolver.SolverFactory;
import scpsolver.problems.LPSolution;
import scpsolver.problems.LPWizard;
import scpsolver.problems.LPWizardConstraint;
import weka.classifiers.functions.LibLINEAR;
import weka.classifiers.functions.SMOreg;
import weka.classifiers.trees.M5P;
import weka.core.Instance;
import weka.core.Utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class ILPSolverGurobi extends AbstractSolver {

  private double[] costArrayNoRegression;
  private double[] costArraySMO;
  private double[] costArrayLinear;
  private double[] costArrayM5P;
  private double[][] rawCostArray;
  private int numQuery;
  private int numCostVariables;
  private Set<String> structureStrSet;
  private List<String> structureStrList;
  private static final String[] REGRESSION_STRINGS = {"NoRegression", "M5P"};
  private static final double MAX_QUERY_TIME = 1000000;

  public ILPSolverGurobi(Connection conn, Workload workload, Schema schema,
                         Set<Configuration> configurations,
                         List<SampleInfo> sampleDBs,
                         DatabaseInfo dbInfo,
                         FeatureExtractor extractor, boolean useRegression) {

    super(conn, workload, schema, configurations, sampleDBs, dbInfo, extractor, useRegression);
    this.numCostVariables = workload.getTotalConfigurationCount();
    this.rawCostArray = new double[sampleDBs.size()]
        [numCostVariables];
    this.costArrayNoRegression = new double[numCostVariables];
    this.costArraySMO = new double[numCostVariables];
    this.costArrayLinear = new double[numCostVariables];
    this.costArrayM5P = new double[numCostVariables];
    this.numQuery = workload.getQueries().size();
    this.structureStrSet = new HashSet<>();
  }

  public boolean solve() {
    // fill cost array with DOUBLE_MAX
    Arrays.fill(this.costArrayNoRegression, this.MAX_QUERY_TIME);
    Arrays.fill(this.costArrayM5P, this.MAX_QUERY_TIME);
    for (int i = 0; i < sampleDBs.size(); ++i) {
      Arrays.fill(this.rawCostArray[i], this.MAX_QUERY_TIME);
    }
    Map<Structure, Double> structureSizeMap = new HashMap<>();
    ArrayList<TemporalCostArray> temporalCostArrays = new ArrayList<>();
    this.sizeLimits = GPDMain.userInput.getSetting().getSizeLimits();
    Stopwatch entireTime = Stopwatch.createStarted();

    int count = 0;
    List<Structure> possibleStructures = getAllStructures(configurations);
    for (Structure s : possibleStructures) {
      structureStrSet.add(s.getNonUniqueString());
    }
    structureStrList = new ArrayList(structureStrSet);
    Date date = new Date();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-hhmmss");
    String formattedDate = sdf.format(date);

    // fill the cost array first.
    Stopwatch timetoFillCostArray = Stopwatch.createStarted();
    GPDLogger.info(this, String.format("Filling the cost & size array with " +
        "%d configurations.", configurations.size()));
    if (!fillCostAndSizeArray(temporalCostArrays)) {
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

    for (String regressionStr : this.REGRESSION_STRINGS) {
      GPDLogger.info(this, "Solving for " + regressionStr);
      ArrayList<TemporalCostArray> costArrays = new ArrayList<>();
      for (TemporalCostArray array : temporalCostArrays) {
        if (array.getType().equalsIgnoreCase(regressionStr)) {
          costArrays.add(array);
        }
      }
      for (TemporalCostArray tca : costArrays) {
        for (long sizeLimit : sizeLimits) {

          double[] costArray = tca.getArray();
          try {
            Map<String, GRBVar> xVarMap = new HashMap<>();
            Map<String, GRBVar> yVarMap = new HashMap<>();
            GRBEnv env = new GRBEnv("gurobi.log");
            GRBModel model = new GRBModel(env);
            model.set(GRB.DoubleParam.TimeLimit, GPDMain.userInput.getSetting().getIlpTimeLimit());

            // Set objective
            GRBLinExpr obj = new GRBLinExpr();
            count = 0;
            for (int i = 0; i < numQuery; ++i) {
              Query q = workload.getQueries().get(i);
              int numConfig = q.getConfigurations().size();
              for (int j = 0; j < numConfig; ++j) {
                String varName = "x_" + i + "_" + j;
                GRBVar x = model.addVar(0.0, 1.0, 0.0, GRB.BINARY, varName);
                xVarMap.put(varName, x);
                obj.addTerm(costArray[count], x);
                ++count;
              }
            }
            model.setObjective(obj, GRB.MINIMIZE);

            // Add constraints
            for (int i = 0; i < numQuery; ++i) {
              Query q = workload.getQueries().get(i);
              int numConfig = q.getConfigurations().size();
              String constName = "c_1_" + i;
              GRBLinExpr cons = new GRBLinExpr();
              for (int j = 0; j < numConfig; ++j) {
                String varName = "x_" + i + "_" + j;
                GRBVar x = xVarMap.get(varName);
                cons.addTerm(1.0, x);
              }
              model.addConstr(cons, GRB.EQUAL, 1.0, constName);
            }

            // Add constraints for x_ij <= y_t
            int constraintCount = 0;
            for (int t = 0; t < possibleStructures.size(); ++t) {
              Structure y = possibleStructures.get(t);
              String yVarName = "y_" + t;
              for (int i = 0; i < numQuery; ++i) {
                Query q = workload.getQueries().get(i);
                int numConfig = q.getConfigurations().size();
                for (int j = 0; j < numConfig; ++j) {
                  Configuration config = q.getConfigurationList().get(j);
                  for (Structure s : config.getStructures()) {
                    if (s.getName().equals(y.getName())) {
                      String constName = "c_2_" + constraintCount;
                      GRBLinExpr cons = new GRBLinExpr();
                      String xVarName = "x_" + i + "_" + j;
                      GRBVar xVar = xVarMap.get(xVarName);
                      GRBVar yVar = yVarMap.containsKey(yVarName) ?
                          yVarMap.get(yVarName) : model.addVar(0.0, 1.0, 0.0, GRB.BINARY, yVarName);
                      yVarMap.put(yVarName, yVar);
                      cons.addTerm(1.0, xVar);
                      cons.addTerm(-1.0, yVar);
                      model.addConstr(cons, GRB.LESS_EQUAL, 0.0, constName);
                      ++constraintCount;
                    }
                  }
                }
              }
            }

            // TODO: add constraints for compatibility matrix

            // if size limit exists, add it as a constraint
            if (sizeLimit > 0) {
              GRBLinExpr cons = new GRBLinExpr();
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
              boolean hasModelBuilt = false;
              GPDClassifier sr = new GPDClassifier(smo);
              for (int j = 0; j < numStructures; ++j) {
                String var = "y_" + j;
                Structure s = possibleStructures.get(j);
                double structureSize = 0;
                if (structureSizeMap.containsKey(s)) {
                  structureSize = structureSizeMap.get(s).doubleValue();
                } else {
                  if (!hasModelBuilt) {
                    sr.build(extractor.getTrainDataForSize());
                    hasModelBuilt = true;
                  }
                  Instance testInstance = extractor.getTestInstanceForSize(
                      dbInfo.getTargetDBName(), schema, s);
                  structureSize = sr.regress(testInstance);
                  structureSizeMap.put(s, structureSize);
                }
                GPDLogger.info(this, String.format("Estimated Structure Size = %f (%s)",
                    structureSize, s.getQueryString()));
                GRBVar y = yVarMap.get(var);
                cons.addTerm(structureSize, y);
              }
              model.addConstr(cons, GRB.LESS_EQUAL, sizeLimit, "c_size");
            }

            File modelDir = new File("./models/");
            modelDir.mkdirs();
            model.write(String.format("./models/%s-%s-%d-%d-gurobi-model.lp", dbInfo.getTargetDBName(), regressionStr, tca.getTimeTaken(), sizeLimit));

            // now solve
            Stopwatch timeToSolve = Stopwatch.createStarted();
            model.optimize();
            GRBVar[] vars = model.getVars();
            double[] varValues = model.get(GRB.DoubleAttr.X, vars);
            String[] varNames = model.get(GRB.StringAttr.VarName, vars);
            for (int i = 0; i < vars.length; ++i) {
              GPDLogger.debug(this, varNames[i] + " = " + varValues[i]);
            }


            GPDLogger.info(this, "Objective Value = " + model.get(GRB.DoubleAttr.ObjVal));
            timeTaken = timeToSolve.elapsed(TimeUnit.SECONDS);
            GPDLogger.info(this,
                String.format("took %d seconds to solve the problem.", timeTaken));

            Set<Structure> optimalStructures = new LinkedHashSet<>();
            for (int t = 0; t < possibleStructures.size(); ++t) {
              String varName = "y_" + t;
              GRBVar y = yVarMap.get(varName);
              if (y.get(GRB.DoubleAttr.X) > 0) {
                optimalStructures.add(possibleStructures.get(t));
              }
            }

            System.out.println("Optimal structures with " + regressionStr
                + ", runTime = " + tca.getTimeTaken() + ", sizeLimit = " + sizeLimit + " :");
            File resultDir = new File("./indexes/" + formattedDate);
            resultDir.mkdirs();
            BufferedWriter resultWriter =
                new BufferedWriter(
                    new FileWriter(
                        new File(
                            String.format("./indexes/%s/%s_%s_%d_%d.index", formattedDate,
                                dbInfo.getTargetDBName(), regressionStr, tca.getTimeTaken(), sizeLimit))));
            for (Structure s : optimalStructures) {
              System.out.println("\t"+s.getQueryString());
              resultWriter.write(s.getQueryString() + "\n");
            }
            resultWriter.close();

            model.dispose();
            env.dispose();

          } catch (GRBException e) {
            GPDLogger.error(this, "Gurobi error.");
            e.printStackTrace();
          } catch (IOException ex) {
            GPDLogger.error(this, "IOException");
            ex.printStackTrace();
          }
        }
      }
    }
    timeTaken = entireTime.elapsed(TimeUnit.SECONDS);
    GPDLogger.info(this,
        String.format("took %d seconds for the entire process.",
            timeTaken));
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

  private boolean fillCostArray() {
    // build classifier for cost regression
    SMOreg smo = new SMOreg();
    LibLINEAR libLINEAR = new LibLINEAR();
    M5P m5p = new M5P();
//    LibSVM libSVM = new LibSVM();
//    libSVM.setSVMType(new SelectedTag(LibSVM.SVMTYPE_EPSILON_SVR, LibSVM.TAGS_SVMTYPE));
//    libSVM.setCacheSize(4096);
    List<Query> queries = workload.getQueries();
    libLINEAR.setDebug(true);
    try {
      libLINEAR.setOptions(Utils.splitOptions("-S 0"));
      smo.setOptions(Utils.splitOptions("-C 1.0 -N 0 " +
          "-I \"weka.classifiers.functions.supportVector.RegSMOImproved " +
          "-T 0.001 -V -P 1.0E-12 -L 0.001 -W 1\" " +
          "-K \"weka.classifiers.functions.supportVector.PolyKernel -E 1.0 -C 0\""));
    } catch (Exception e) {
      GPDLogger.error(this, "Failed to set options for the classifier.");
      e.printStackTrace();
      return false;
    }
    GPDClassifier m5pClassifier = new GPDClassifier(m5p);

    if (!m5pClassifier.build(extractor.getTrainData())) {
      return false;
    }
    int count = 0;
    Date date = new Date();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-hhmmss");
    String formattedDate = sdf.format(date);
    BufferedWriter noRegCostWriter = null, m5pCostWriter = null;
    try {
      File resultDir = new File("./results");
      resultDir.mkdirs();
      noRegCostWriter = new BufferedWriter(new FileWriter(new File("./results/noreg-" + formattedDate)));
      m5pCostWriter = new BufferedWriter(new FileWriter(new File("./results/m5p-" + formattedDate)));
    } catch (IOException e) {
      e.printStackTrace();
    }
    for (int i = 0; i < queries.size(); ++i) {
      Query q = queries.get(i);
      for (Configuration config : q.getConfigurations()) {
        int configId = 0;
        int structureCount = 0;
        int structureSize = structureStrList.size();
        for (Structure s : config.getStructures()) {
          configId += Math.pow(structureSize, structureCount) *
              structureStrList.indexOf(s.getNonUniqueString());
          ++structureCount;
        }
        Instance testInstance = extractor.getTestInstance(dbInfo.getTargetDBName(),
            schema, q, configId);
        costArrayM5P[count] = m5pClassifier.regress(testInstance);
        long total = 0;
        for (int d = 0; d < numSampleDBs; ++d) {
          total += rawCostArray[d][count];
        }
        costArrayNoRegression[count] = total / numSampleDBs;
        try {
          noRegCostWriter.write(String.valueOf(costArrayNoRegression[count]) + "\n");
          m5pCostWriter.write(String.valueOf(costArrayM5P[count]) + "\n");
        } catch (IOException e) {
          e.printStackTrace();
        }
        ++count;
      }
    }
    try {
      noRegCostWriter.close();
      m5pCostWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return true;
  }

  private boolean fillCostAndSizeArray(List<TemporalCostArray> costArrays) {

    GPDLogger.info(this, String.format(
        "Filling the cost array."));
    Stopwatch stopwatch;
    Stopwatch runTime;
    List<Query> queries = workload.getQueries();
    long incrementalRunTime = GPDMain.userInput.getSetting().getIncrementalRunTime();
    long nextRunTime = incrementalRunTime;
    boolean isIncrementalRun = GPDMain.userInput.getSetting().isIncrementalRun();

    extractor.initialize(sampleDBs, dbInfo.getTargetDBName(), schema,
        new ArrayList<>(structureStrSet));

    runTime = Stopwatch.createStarted();

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
              "Building structures for configuration #%d out of %d. (sample DB #%d out of #%d)", configuration.getId() + 1,
              numCostVariables, d+1, numSampleDBs));
          for (Structure s : configuration.getStructures()) {
            s.create(conn);
            if (trainedSet.add(s)) {
              extractor.addTrainingDataForSize(dbName, schema, s);
              GPDLogger.debug(this,
                  String.format("Added training data for size: %s = %d @ %s", s.getQueryString(), s.getSize(), dbName));
            }
          }

          GPDLogger.info(this, String.format(
              "Running queries for configuration #%d out of %d. (sample DB #%d out of #%d)", configuration.getId() + 1,
              numCostVariables, d+1, numSampleDBs));
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
          // when query times out, we assign 'MAX_QUERY_TIME' seconds to its cost.
          if (isTimedOut) {
            rawCostArray[d][count] = (long) MAX_QUERY_TIME;
          }
          else {
            rawCostArray[d][count] = (long) queryTime;
          }
          if (useRegression) {
            int configId = 0;
            int structureCount = 0;
            int structureSize = structureStrList.size();
            for (Structure s : configuration.getStructures()) {
              configId += Math.pow(structureSize, structureCount) *
                  structureStrList.indexOf(s.getNonUniqueString());
              ++structureCount;
            }
            extractor.addTrainingData(dbName, schema, q,
                configId,
                queryTime);
          }

          // remove structures
          GPDLogger.info(this, String.format(
              "Removing structures for configuration #%d out of %d. (sample DB #%d out of #%d)", configuration.getId()+1,
              numCostVariables, d+1, numSampleDBs));
          for (Structure s : configuration.getStructures()) {
            s.drop(conn);
          }
          ++count;
          long elapsed = runTime.elapsed(TimeUnit.SECONDS);
          if (isIncrementalRun && elapsed >= nextRunTime) {
            // create cost array for the time.
            GPDLogger.info(this, "Incrementally filling cost array for time = " + elapsed);
            if (fillCostArray()) {
              double[] noRegression = Arrays.copyOf(costArrayNoRegression, costArrayNoRegression.length);
              double[] m5p = Arrays.copyOf(costArrayM5P, costArrayM5P.length);
              TemporalCostArray noRegressionArray = new TemporalCostArray(noRegression, elapsed, "NoRegression");
              TemporalCostArray m5pArray = new TemporalCostArray(m5p, elapsed, "M5P");
              costArrays.add(noRegressionArray);
              costArrays.add(m5pArray);
            } else {
              GPDLogger.error(this, "Failed to fill cost array.");
              return false;
            }
            GPDLogger.info(this, "Incrementally filled cost array for time = " + elapsed);
            nextRunTime += incrementalRunTime;
            // reset catalog.
            try {
              conn.setCatalog(dbName);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      }
    }

    long elapsed = runTime.elapsed(TimeUnit.SECONDS);
    if (fillCostArray()) {
      double[] noRegression = Arrays.copyOf(costArrayNoRegression, costArrayNoRegression.length);
      double[] m5p = Arrays.copyOf(costArrayM5P, costArrayM5P.length);
      TemporalCostArray noRegressionArray = new TemporalCostArray(noRegression, elapsed, "NoRegression");
      TemporalCostArray m5pArray = new TemporalCostArray(m5p, elapsed, "M5P");
      costArrays.add(noRegressionArray);
      costArrays.add(m5pArray);
    } else {
      GPDLogger.error(this, "Failed to fill cost array.");
      return false;
    }
    return true;
  } // end fillCostAndSizeArray()

}
