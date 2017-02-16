package edu.umich.gpd.util;

import edu.umich.gpd.database.Structure;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class UtilFunctions {
  public static boolean containsStructureWithDuplicateTables(Set<Structure> configuration) {
    HashSet<String> tableNameSet = new HashSet<>();
    for (Structure s : configuration) {
      if (tableNameSet.contains(s.getTable().getName())) {
        return true;
      } else {
        tableNameSet.add(s.getTable().getName());
      }
    }
    return false;
  }
}
