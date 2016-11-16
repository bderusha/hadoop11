package Classifier;

public class LemmaInfo {
	private int index;
	private int count = 0;
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	public int getIndex() {
		return index;
	}
	
	public void addCount(int count) {
		this.count += count;
	}
	
	public int getCount() {
		return count;
	}
}
