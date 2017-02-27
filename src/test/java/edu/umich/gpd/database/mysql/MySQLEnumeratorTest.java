package edu.umich.gpd.database.mysql;

import edu.umich.gpd.parser.SchemaParser;
import edu.umich.gpd.parser.WorkloadParser;
import edu.umich.gpd.database.common.Structure;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.workload.Workload;

import java.io.File;
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

    List<Set<Structure>> configurations = enumerator.enumerateStructures(s,w);
    if (configurations == null) {
      return;
    }

    int count = 1;
    for (Set<Structure> configuration : configurations) {
      System.out.println("Configuration " + count + ":");
      for (Structure st : configuration) {
        if (st instanceof MySQLIndex) {
          MySQLIndex ind = (MySQLIndex)st;
          System.out.println("\t" + ind.toString());
        } else if (st instanceof MySQLUniqueIndex) {
          MySQLUniqueIndex ind = (MySQLUniqueIndex)st;
          System.out.println("\t" + ind.toString());
        }
      }
      ++count;
    }
  }
}
