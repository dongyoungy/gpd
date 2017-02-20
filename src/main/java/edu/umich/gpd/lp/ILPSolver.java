package edu.umich.gpd.lp;

import com.esotericsoftware.minlog.Log;
import com.google.common.base.Stopwatch;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.workload.Query;
import edu.umich.gpd.workload.Workload;

import scpsolver.problems.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class ILPSolver {
  private Connection conn;
  private Workload workload;
  private List<Set<Structure>> configurations;
  private long[][] costArray;
  private int numQuery;
  private int numConfiguration;


  public ILPSolver(Connection conn, Workload workload, List<Set<Structure>> configurations) {
    this.conn = conn;
    this.workload = workload;
    this.configurations = configurations;
    this.costArray = new long[workload.getQueries().size()][configurations.size()];
    this.numQuery = workload.getQueries().size();
    this.numConfiguration = configurations.size();
  }

  public void solve() {
    Stopwatch entireTime = Stopwatch.createStarted();
    // fill the cost array first.
    try {
      Stopwatch timetoFillCostArray = Stopwatch.createStarted();
      fillCostArray();
      long timeTaken = timetoFillCostArray.elapsed(TimeUnit.SECONDS);
      Log.info(this.getClass().getCanonicalName(), String.format("took %d seconds to fill" +
          " the cost array.", timeTaken));
    } catch (SQLException e) {
      e.printStackTrace();
      Log.error(this.getClass().getCanonicalName(), "Failed to fill cost array.");
      return;
    }
    List<Structure> possibleStructures = getPossibleStructures(configurations);
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
        Set<Structure> structureSet = configurations.get(j);
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

    // TODO: add constraints for structure size

    // now solve
    Stopwatch timeToSolve = Stopwatch.createStarted();
    LPSolution solution = lpw.solve();
    System.out.println("Objective Value = " + solution.getObjectiveValue());
    long timeTaken = timeToSolve.elapsed(TimeUnit.SECONDS);
    Log.info(this.getClass().getCanonicalName(), String.format("took %d seconds to solve the problem.", timeTaken));
    timeTaken = entireTime.elapsed(TimeUnit.SECONDS);
    Log.info(this.getClass().getCanonicalName(), String.format("took %d seconds for the entire process.",
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
    System.out.println("Optimal structures:");
    for (int t = 0; t < numStructures; ++t) {
      String varName = "y_" + t;
      if (solution.getInteger(varName) == 1) {
        System.out.println("\t"+possibleStructures.get(t).getQueryString());
      }
    }
  }

  private void buildCompatibilityMatrix(List<Structure> possibleStructures,
                                        boolean[][] compatibilityMatrix) {
    // initialize.
    for (int i = 0; i < possibleStructures.size(); ++i) {
      for (int j = 0; j < possibleStructures.size(); ++j) {
        compatibilityMatrix[i][j] = true;
      }
    }

    for (int i = 0; i < possibleStructures.size(); ++i) {
      Structure s1 = possibleStructures.get(i);
      for (int j = i + 1; j < possibleStructures.size(); ++j) {
        Structure s2 = possibleStructures.get(j);
        if (s1.getTable().getName().equals(s2.getTable().getName())) {
          compatibilityMatrix[i][j] = false;
          compatibilityMatrix[j][j] = false;
        }
      }
    }
  }

  private List<Structure> getPossibleStructures(List<Set<Structure>> configurations) {
    Set<Structure> possibleStructures = new HashSet<>();
    for (Set<Structure> structures : configurations) {
      for (Structure s : structures) {
        possibleStructures.add(s);
      }
    }
    return new ArrayList(possibleStructures);
  }

  private void fillCostArray() throws SQLException {

    Log.info(this.getClass().getCanonicalName(), String.format(
        "Filling the cost array"));
    Stopwatch stopwatch;
    Statement stmt = conn.createStatement();
    List<Query> queries = workload.getQueries();
    for (int j = 0; j < configurations.size(); ++j) {
      Set<Structure> configuration = configurations.get(j);
      // build structures
      Log.info(this.getClass().getCanonicalName(), String.format(
          "Building structures for configuration #%d out of %d.", j+1, configurations.size()));
      for (Structure s : configuration) {
        s.create(conn);
      }

      Log.info(this.getClass().getCanonicalName(), String.format(
          "Running queries for configuration #%d out of %d.", j+1, configurations.size()));
      for (int i = 0; i < queries.size(); ++i) {
        Query q = queries.get(i);
        stopwatch = Stopwatch.createStarted();
        stmt.execute(q.getContent());
        costArray[i][j] = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      }

      // remove structures
      Log.info(this.getClass().getCanonicalName(), String.format(
          "Removing structures for configuration #%d out of %d.", j+1, configurations.size()));
      for (Structure s : configuration) {
        s.drop(conn);
      }
    }
  }
}
