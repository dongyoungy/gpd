package edu.umich.gpd.workload;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public class Query {

  private static int idCount = 1;
  int id;
  private String content;

  // do not allow using default constructor
  private Query() {

  }

  public Query(String content) {
    this.id = idCount++;
    this.content = content;
  }

  public String getContent() {
    return content;
  }

  public int getId() {
    return id;
  }
}

