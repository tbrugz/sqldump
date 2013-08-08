package tbrugz.sqldiff.model;

public enum ChangeType {
	ADD, ALTER, DROP, RENAME, REPLACE;
	
	public ChangeType inverse() {
		switch (this) {
		case ADD:
			return DROP;
		case DROP:
			return ADD;
		case ALTER:
		case RENAME:
		case REPLACE:
			return this;
		}
		throw new IllegalStateException("Unknown ChangeType: "+this);
	}
}
