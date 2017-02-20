package edu.umich.gpd.util;

import com.esotericsoftware.minlog.Log;

/**
 * Created by Dong Young Yoon on 2/19/17.
 */
public class GPDLogger extends Log.Logger {

  private static GPDLogger instance = null;

  protected GPDLogger() {

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
        aClass.getCanonicalName() + ":" + aClass.getEnclosingMethod().getName(),
        message,
        ex);
  }

  public void log(int level, Class aClass, String message) {
    super.log(level,
        aClass.getCanonicalName() + ":" + aClass.getEnclosingMethod().getName(),
        message,
        null);
  }

  public void info(Class aClass, String message) {
    super.log(Log.LEVEL_INFO,
        aClass.getCanonicalName() + ":" + aClass.getEnclosingMethod().getName(),
        message,
        null);
  }

  public void debug(Class aClass, String message) {
    super.log(Log.LEVEL_DEBUG,
        aClass.getCanonicalName() + ":" + aClass.getEnclosingMethod().getName(),
        message,
        null);
  }

  public void error(Class aClass, String message) {
    super.log(Log.LEVEL_ERROR,
        aClass.getCanonicalName() + ":" + aClass.getEnclosingMethod().getName(),
        message,
        null);
  }
}
