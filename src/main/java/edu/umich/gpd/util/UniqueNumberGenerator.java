package edu.umich.gpd.util;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class UniqueNumberGenerator {
  private static final long LIMIT = 10000000000L;
  private static long last = 0;
  public static synchronized long getUniqueID() {
    long id = System.currentTimeMillis() % LIMIT;
    if (id <= last) {
      id = (last + 1) % LIMIT;
    }
    return last = id;
  }
}
