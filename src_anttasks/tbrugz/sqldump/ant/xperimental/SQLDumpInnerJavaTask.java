package tbrugz.sqldump.ant.xperimental;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.taskdefs.Java;
import org.apache.tools.ant.types.Path;

import tbrugz.sqldump.SQLDump;

//XXX: when using inner java task, how to set/inherit ant properties?
public class SQLDumpInnerJavaTask extends Task {

	static final String CLASSNAME = SQLDump.class.getName();

	Java java;
	Path classpath;

	void setup() throws BuildException {
		java = (Java) getProject().createTask("java");
		java.setTaskName(getTaskName());
	}

	//TODO: set parameters, set classpathref
	@Override
	public void execute() throws BuildException {
		setup();
		java.setClassname(CLASSNAME);
		if(getClasspath()!=null) {
			java.setClasspath(getClasspath());
		}
		//java.setFork(true);
		java.setFailonerror(true);
		java.execute();
	}

	public Path getClasspath() {
		return classpath;
	}

	public void setClasspath(Path classpath) {
		this.classpath = classpath;
	}

}
