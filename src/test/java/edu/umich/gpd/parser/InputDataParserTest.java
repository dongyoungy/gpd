package edu.umich.gpd.parser;

import edu.umich.gpd.userinput.InputData;

import java.io.File;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class InputDataParserTest {
  public static void main(String[] args) {
    File file = new File("/Users/dyoon/work/gpd/examples/sample.json");
    InputData data = InputDataParser.parse(file);
    System.out.println(data.toString());
  }
}
