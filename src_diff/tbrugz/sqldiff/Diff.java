package tbrugz.sqldiff;

public interface Diff {
	public ChangeType getChangeType();
	public String getDiff();
}
