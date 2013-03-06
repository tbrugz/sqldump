package tbrugz.sqldump.def;

public abstract class AbstractFailable {

	public static boolean DEFAULT_FAILONERROR = false; //XXX: default 'failonerror' should be true?
	
	protected boolean failonerror = DEFAULT_FAILONERROR;
	
	public void setFailOnError(boolean failonerror) {
		this.failonerror = failonerror;
	}

}
