package edu.umich.gpd.database;

import edu.umich.gpd.database.Structure;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.workload.Workload;

import java.util.List;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2/13/17.
 *
 * Enumerates all possible interesting physical structures from the workload
 */
public abstract class StructureEnumerator {
  public abstract List<Set<Structure>> enumerateStructures(Schema s, Workload w);
}
