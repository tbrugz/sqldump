package tbrugz.sqldump.ant.xperimental;

import java.io.File;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.Java;

import tbrugz.sqldump.SQLDump;

//XXX: when using java task, how to set/inherit ant properties?
public class SQLDumpJavaTask extends Java {

	@Override
	public void setClassname(String s) throws BuildException {
		throw new IllegalArgumentException(s);
	}
	
	@Override
	public void setJar(File jarfile) throws BuildException {
		throw new IllegalArgumentException(String.valueOf(jarfile));
	}
	
	@Override
	public void execute() throws BuildException {
		super.setClassname(SQLDump.class.getName());
		super.execute();
	}
}
