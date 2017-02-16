package edu.umich.gpd.parser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import edu.umich.gpd.userinput.InputData;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class InputDataParser {
  public static InputData parse(File file) {
    Gson gson = new GsonBuilder().create();
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(file);
      byte[] data = new byte[(int) file.length()];
      fis.read(data);
      fis.close();

      String rawJson = new String(data, "UTF-8");

      InputData inputData = gson.fromJson(rawJson, InputData.class);
      return inputData;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return null;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}
