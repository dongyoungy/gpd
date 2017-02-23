package edu.umich.gpd.regression;

import com.esotericsoftware.minlog.Log;
import weka.classifiers.functions.SMOreg;
import weka.core.Instance;
import weka.core.Instances;

/**
 * Created by Dong Young Yoon on 2/22/17.
 */
public class GPDRegression {

  public static double regressSMOReg(Instances trainData, Instance testInstance) {
    SMOreg sr = new SMOreg();
    try {
      sr.buildClassifier(trainData);
      return sr.classifyInstance(testInstance);
    } catch (Exception e) {
      Log.error("GPDRegression", "Error while performing regression wiht SMOReg.");
      e.printStackTrace();
      return -1;
    }
  }
}
