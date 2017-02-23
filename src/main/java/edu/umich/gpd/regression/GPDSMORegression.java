package edu.umich.gpd.regression;

import com.esotericsoftware.minlog.Log;
import weka.classifiers.functions.SMOreg;
import weka.core.Instance;
import weka.core.Instances;

/**
 * Created by Dong Young Yoon on 2/22/17.
 */
public class GPDSMORegression {

  private SMOreg sr;

  public GPDSMORegression() {
    sr = new SMOreg();
  }

  public boolean build(Instances trainData) {
    try {
      sr.buildClassifier(trainData);
    } catch (Exception e) {
      Log.error("GPDRegression", "Error while building classifier with SMOReg.");
      e.printStackTrace();
      return false;
    }
    return true;
  }


  public double regress(Instance testInstance) {
    try {
      return sr.classifyInstance(testInstance);
    } catch (Exception e) {
      Log.error("GPDRegression", "Error while performing regression wiht SMOReg.");
      e.printStackTrace();
      return -1;
    }
  }
}
