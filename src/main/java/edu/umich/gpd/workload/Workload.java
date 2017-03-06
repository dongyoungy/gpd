package edu.umich.gpd.workload;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public class Workload {
  private List<Query> queries;

  public Workload() {
    queries = new ArrayList<>();
  }

  public void addQuery(Query q) {
    queries.add(q);
  }

  public List<Query> getQueries() {
    return queries;
  }

  public int getTotalConfigurationCount() {
    int count = 0;
    for (Query q : queries) {
      count += q.getConfigurations().size();
    }
    return count;
  }
}
