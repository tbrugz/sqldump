package tbrugz.sqldump.def;

public abstract class AbstractFailable {

	public static final boolean DEFAULT_FAILONERROR = true;
	
	protected boolean failonerror = DEFAULT_FAILONERROR;
	
	public void setFailOnError(boolean failonerror) {
		this.failonerror = failonerror;
	}

}
