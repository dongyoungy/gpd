package edu.umich.gpd.database.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dong Young Yoon on 3/5/17.
 */
public class Configuration {

  private static int idCount = 0;
  private int id;
  private List<Structure> structures;

  public Configuration() {
    id = idCount++;
    structures = new ArrayList<>();
  }

  public Configuration(List<Structure> structures) {
    id = idCount++;
    this.structures = structures;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Configuration that = (Configuration) o;

    if (id != that.id) return false;
    return structures != null ? structures.equals(that.structures) : that.structures == null;
  }

  @Override
  public int hashCode() {
    int result = id;
    result = 31 * result + (structures != null ? structures.hashCode() : 0);
    return result;
  }

  public int getId() {
    return id;
  }

  public List<Structure> getStructures() {
    return structures;
  }
}
