package edu.umich.gpd.database.hive;

import com.esotericsoftware.minlog.Log;
import edu.umich.gpd.database.common.Configuration;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.database.mysql.MySQLEnumerator;
import edu.umich.gpd.database.mysql.MySQLIndex;
import edu.umich.gpd.database.mysql.MySQLUniqueIndex;
import edu.umich.gpd.main.GPDMain;
import edu.umich.gpd.parser.InputDataParser;
import edu.umich.gpd.parser.SchemaParser;
import edu.umich.gpd.parser.WorkloadParser;
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
public class HiveEnumeratorTest {
  public static void main(String[] args) {
    File file = new File("/Users/dyoon/work/gpd/examples/tpch-schema.sql");
    SchemaParser parser = new SchemaParser("@@@");
    Schema s = parser.parse(file);
    file = new File("/Users/dyoon/work/gpd/examples/tpch-workload.sql");
    WorkloadParser parser2 = new WorkloadParser("@@@");
    Workload w = parser2.parse(file);

    HiveEnumerator enumerator = new HiveEnumerator();

    file = new File("/Users/dyoon/work/gpd/examples/sample.json");
    GPDMain.userInput = InputDataParser.parse(file);

    GPDMain.userInput.getSetting().setMaxNumColumn(300);
    GPDMain.userInput.getSetting().setDebug(true);
    GPDMain.userInput.getSetting().setMaxNumColumnPerStructure(2);
    GPDMain.userInput.getSetting().setMaxNumStructurePerTable(2);
    GPDMain.userInput.getSetting().setMaxNumTablePerQuery(2);
//    GPDMain.userInput.getDatabaseInfo().getAvailableStructures().add(
//        new StructureInfo("unique_index", "customer" ,"c_custkey")
//    );
//    Set<Configuration> configurations = enumerator.enumerateStructures(s,w);
//    while (configurations == null) {
//      int prevMaxNumColumn = GPDMain.userInput.getSetting().getMaxNumColumn();
//
//      Log.info("MySQLEnumeratorTest",
//          "Reducing the number of columns to consider to: " +
//          (prevMaxNumColumn-1));
//      configurations = enumerator.enumerateStructures(s,w);
//    }

    int count = 1;
    Set<Structure> possibleStructure = enumerator.getStructures(s,w);
    System.out.println("Possible Structures:");
    for (Structure st : possibleStructure) {
      if (st instanceof HiveBitmapIndex) {
        HiveBitmapIndex ind = (HiveBitmapIndex)st;
        System.out.println("\t" + ind.getNonUniqueString());
      } else if (st instanceof HiveCompactIndex) {
        HiveCompactIndex ind = (HiveCompactIndex)st;
        System.out.println("\t" + ind.getNonUniqueString());
      }
    }
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
