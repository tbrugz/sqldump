package tbrugz.sqldump.def;

public abstract class AbstractFailable {

	protected boolean failonerror = false; //XXX: default 'failonerror' should be true?
	
	public void setFailOnError(boolean failonerror) {
		this.failonerror = failonerror;
	}

}
