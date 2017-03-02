package edu.umich.gpd.classifier;

import com.esotericsoftware.minlog.Log;
import weka.classifiers.Classifier;
import weka.classifiers.functions.SMOreg;
import weka.core.Instance;
import weka.core.Instances;

/**
 * Created by Dong Young Yoon on 2/22/17.
 */
public class GPDClassifier {

  private Classifier classifier;

  public GPDClassifier(Classifier classifier) {
    this.classifier = classifier;
  }

  public boolean build(Instances trainData) {
    try {
      // assuming the last attribute is the class attribute
      trainData.setClassIndex(trainData.numAttributes() - 1);
      classifier.buildClassifier(trainData);
    } catch (Exception e) {
      Log.error("GPDRegression", "Error while building classifier with SMOReg.");
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public double regress(Instance testInstance) {
    try {
      return classifier.classifyInstance(testInstance);
    } catch (Exception e) {
      Log.error("GPDRegression", "Error while performing regression with SMOReg.");
      e.printStackTrace();
      return -1;
    }
  }
}
