package edu.umich.gpd.database.mysql;

import com.esotericsoftware.minlog.Log;
import edu.umich.gpd.database.common.Configuration;
import edu.umich.gpd.main.GPDMain;
import edu.umich.gpd.parser.SchemaParser;
import edu.umich.gpd.parser.WorkloadParser;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.workload.Workload;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class MySQLEnumeratorTest {
  public static void main(String[] args) {
    File file = new File("/Users/dyoon/work/gpd/examples/tpch-schema.sql");
    SchemaParser parser = new SchemaParser("@@@");
    Schema s = parser.parse(file);
    file = new File("/Users/dyoon/work/gpd/examples/tpch-workload.sql");
    WorkloadParser parser2 = new WorkloadParser("@@@");
    Workload w = parser2.parse(file);

    MySQLEnumerator enumerator = new MySQLEnumerator();

    GPDMain.userInput.getSetting().setMaxNumColumn(100);
    GPDMain.userInput.getSetting().setDebug(true);
    GPDMain.userInput.getSetting().setMaxNumColumnPerStructure(3);
    Set<Configuration> configurations = enumerator.enumerateStructures(s,w);
    while (configurations == null) {
      int prevMaxNumColumn = GPDMain.userInput.getSetting().getMaxNumColumn();

      Log.info("MySQLEnumeratorTest",
          "Reducing the number of columns to consider to: " +
          (prevMaxNumColumn-1));
      configurations = enumerator.enumerateStructures(s,w);
    }

    int count = 1;
    List<Structure> possibleStructure = getPossibleStructures(configurations);
    System.out.println("Possible Structures:");
    for (Structure st : possibleStructure) {
      if (st instanceof MySQLIndex) {
        MySQLIndex ind = (MySQLIndex)st;
        System.out.println("\t" + ind.toString());
      } else if (st instanceof MySQLUniqueIndex) {
        MySQLUniqueIndex ind = (MySQLUniqueIndex)st;
        System.out.println("\t" + ind.toString());
      }
    }
    System.out.println("# of configurations = " + configurations.size());
    System.out.println("# of possible structures = " + possibleStructure.size());
  }

  private static List<Structure> getPossibleStructures(Set<Configuration> configurations) {
    Set<Structure> possibleStructures = new HashSet<>();
    for (Configuration config : configurations) {
      for (Structure s : config.getStructures()) {
        possibleStructures.add(s);
      }
    }
    return new ArrayList(possibleStructures);
  }
}
