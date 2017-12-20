package edu.umich.gpd.classifier;

import com.esotericsoftware.minlog.Log;
import edu.umich.gpd.util.GPDLogger;
import weka.classifiers.Classifier;
import weka.classifiers.functions.SMOreg;
import weka.core.Instance;
import weka.core.Instances;

/**
 * Created by Dong Young Yoon on 2/22/17.
 */
public class GPDClassifier {

  private Classifier classifier;
  private boolean hasBuilt;

  public GPDClassifier(Classifier classifier) {
    this.classifier = classifier;
    this.hasBuilt = false;
  }

  public boolean build(Instances trainData) {
    try {
      // assuming the last attribute is the class attribute
      trainData.setClassIndex(trainData.numAttributes() - 1);
      classifier.buildClassifier(trainData);
      GPDLogger.debug(this, "Built classifier = " + classifier.toString());
      hasBuilt = true;
    } catch (Exception e) {
      Log.error("GPDRegression", "Error while building classifier.");
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public double regress(Instance testInstance) {
    if (!hasBuilt) {
      GPDLogger.error(this, "Need to build the classifier first.");
      return 0;
    }
    try {
      return classifier.classifyInstance(testInstance);
    } catch (Exception e) {
      Log.error("GPDRegression", "Error while performing regression.");
      e.printStackTrace();
      return -1;
    }
  }
}
