package edu.umich.gpd.database.common;

import edu.umich.gpd.workload.Query;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dong Young Yoon on 3/5/17.
 */
public class Configuration implements Comparable {

  private static int idCount = 0;
  private int id;
  private List<Structure> structures;
  private List<Query> queries;

  public Configuration() {
    id = idCount++;
    structures = new ArrayList<>();
    queries = new ArrayList<>();
  }

  public Configuration(List<Structure> structures) {
    id = idCount++;
    this.structures = structures;
    queries = new ArrayList<>();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Configuration that = (Configuration) o;

//    if (id != that.id) return false;
    return structures != null ? structures.equals(that.structures) : that.structures == null;
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = 31 * result + (structures != null ? structures.hashCode() : 0);
    return result;
  }

  public int getId() {
    return id;
  }

  public List<Structure> getStructures() {
    return structures;
  }

  public String getNonUniqueString() {
    String str = "";
    for (Structure s : structures) {
      str += s.getNonUniqueString() + "\n";
    }
    return str;
  }

  public void addQuery(Query q) {
    queries.add(q);
  }

  public List<Query> getQueries() {
    return queries;
  }

  @Override
  public int compareTo(Object o) {
    Configuration other = (Configuration)o;
    if (this.id < other.getId()) return -1;
    else if (this.id > other.getId()) return 1;
    else return 0;
  }
}
