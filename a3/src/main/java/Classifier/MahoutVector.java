package Classifier;

import org.apache.mahout.math.Vector;

public class MahoutVector {
	
	public String classifier;
	public Vector vector;
	
	public void setVector(Vector vector) {
		this.vector = vector;
	}
	
	public Vector getVector() {
		return vector;
	}
	
	public void setClassifier(String classifier) {
		this.classifier = classifier;
	}
	
	public String getClassifier() {
		return classifier;
	}
	
}
