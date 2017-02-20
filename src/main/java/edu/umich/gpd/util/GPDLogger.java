package edu.umich.gpd.util;

import com.esotericsoftware.minlog.Log;

/**
 * Created by Dong Young Yoon on 2/19/17.
 */
public class GPDLogger extends Log.Logger {

  private static GPDLogger instance = null;

  protected GPDLogger() {
    super();
  }

  public static GPDLogger getLogger() {
    if (instance == null) {
      instance = new GPDLogger();
    }
    return instance;
  }

  @Override
  public void log(int level, String category, String message, Throwable ex) {
    super.log(level, category, message, ex);
  }

  public void log(int level, Class aClass, String message, Throwable ex) {
    super.log(level,
        aClass.getSimpleName(),
        message,
        ex);
  }

  public void log(int level, Class aClass, String message) {
    super.log(level,
        aClass.getSimpleName(),
        message,
        null);
  }

  public static void info(Class aClass, String message) {
    Log.info(
        aClass.getSimpleName(),
        message);
  }

  public static void debug(Class aClass, String message) {
    Log.debug(
        aClass.getSimpleName(),
        message);
  }

  public static void error(Class aClass, String message) {
    Log.error(
        aClass.getSimpleName(),
        message);
  }
}
