package edu.umich.gpd.parser;

import edu.umich.gpd.workload.Query;
import edu.umich.gpd.workload.Workload;

import java.io.File;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class WorkloadParserTest {
  public static void main(String[] args) {
    File file = new File("/Users/dyoon/work/gpd/examples/tpcc-workload.sql");
    WorkloadParser parser = new WorkloadParser();
    Workload w = parser.parse(file);
    for (Query q : w.getQueries()) {
      System.out.println(q.getContent());
    }
  }
}
