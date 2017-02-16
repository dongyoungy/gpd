package edu.umich.gpd.userinput;

/**
 * Created by Dong Young Yoon on 2/16/17.
 */
public class Setting {
  private int maxNumColumn;

  public int getMaxNumColumn() {
    return (maxNumColumn == 0) ? 10 : maxNumColumn;
  }
}
