package edu.umich.gpd.util;

import com.esotericsoftware.minlog.Log;

/**
 * Created by Dong Young Yoon on 2/19/17.
 */
public class GPDLogger {

  public static void info(Object anObject, String message) {
    Log.info(
        anObject.getClass().getSimpleName(),
        message);
  }

  public static void debug(Object anObject, String message) {
    Log.debug(
        anObject.getClass().getSimpleName(),
        message);
  }

  public static void error(Object anObject, String message) {
    Log.error(
        anObject.getClass().getSimpleName(),
        message);
  }

  public static void warn(Object anObject, String message) {
    Log.warn(
        anObject.getClass().getSimpleName(),
        message);
  }
}
