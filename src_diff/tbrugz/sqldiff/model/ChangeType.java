package tbrugz.sqldiff.model;

public enum ChangeType {
	ADD, ALTER, RENAME, DROP;
	
	public ChangeType inverse() {
		switch (this) {
		case ADD:
			return DROP;
		case ALTER:
		case RENAME:
			return null;
		case DROP:
			return ADD;
		}
		throw new IllegalStateException("Unknown ChangeType: "+this);
	}
}
