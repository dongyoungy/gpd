package edu.umich.gpd.parser;

import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.schema.Table;

import java.io.File;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class SchemaParserTest {
  public static void main(String[] args) {
    File file = new File("/Users/dyoon/work/gpd/examples/tpcc-schema.sql");
    SchemaParser parser = new SchemaParser("@@@");
    Schema s = parser.parse(file);
    for (Table t : s.getTables()) {
      System.out.println(t.toString());
    }
  }
}
