package edu.umich.gpd.parser;

import edu.umich.gpd.workload.Query;
import edu.umich.gpd.workload.Workload;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by Dong Young Yoon on 2/13/17.
 *
 * this class simply reads a query log file and generates a Workload object filled with Query objects
 */
public class WorkloadParser {

  private String delimiter;

  public WorkloadParser() {
    this.delimiter = "\n";
  }

  public WorkloadParser(String delimiter) {
    this.delimiter = delimiter;
  }

  public Workload parse(File queryFile) {
    try
    {
      FileInputStream fis = new FileInputStream(queryFile);
      byte[] data = new byte[(int)queryFile.length()];
      fis.read(data);
      fis.close();

      Workload w = new Workload();
      String rawQueries = new String(data, "UTF-8");
      String[] queries = rawQueries.split(this.delimiter);
      for (String q : queries) {
        if (!q.isEmpty()) {
          String content = q.replaceAll("$+;", "");
          Query query = new Query(content);
          w.addQuery(query);
        }
      }

      return w;
    }
    catch (FileNotFoundException e)
    {
      e.printStackTrace();
      return null;
    }
    catch (IOException e)
    {
      e.printStackTrace();
      return null;
    }
  }
}
