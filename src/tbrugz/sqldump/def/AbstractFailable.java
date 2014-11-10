package tbrugz.sqldump.def;

import java.io.OutputStream;
import java.io.Writer;

public abstract class AbstractFailable {

	public static final boolean DEFAULT_FAILONERROR = true;
	
	protected boolean failonerror = DEFAULT_FAILONERROR;
	
	public void setFailOnError(boolean failonerror) {
		this.failonerror = failonerror;
	}

	public boolean acceptsOutputWriter() { return false; }
	
	public void setOutputWriter(Writer writer) {
		throw new IllegalStateException();
	}

	public boolean acceptsOutputStream() { return false; }
	
	public void setOutputStream(OutputStream out) {
		throw new IllegalStateException();
	}
	
}
