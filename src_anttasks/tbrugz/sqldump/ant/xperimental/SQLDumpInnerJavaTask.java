package tbrugz.sqldump.ant.xperimental;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.taskdefs.Java;
import org.apache.tools.ant.types.Commandline;
import org.apache.tools.ant.types.Environment;
import org.apache.tools.ant.types.Reference;
import org.apache.tools.ant.types.Environment.Variable;
import org.apache.tools.ant.types.Path;

import tbrugz.sqldump.SQLDump;

//XXXxx: when using inner java task, how to set/inherit ant properties? using system properties...
//XXX: add fork?
public class SQLDumpInnerJavaTask extends Task {

	static final String CLASSNAME = SQLDump.class.getName();

	Java java;
	Path classpath;
	Reference classpathref;
	Boolean failonerror = null;

	Java getJava() {
		if (java == null) {
			java = (Java) getProject().createTask("java");
			java.setTaskName(getTaskName());
		}
		return java;
	}

	void setup() throws BuildException {
		// failonerror
		if(failonerror!=null) {
			addSysproperty("sqldump.failonerror", failonerror?"true":"false");
		}
		// set all properties as system properties
		for(Object key : getProject().getProperties().keySet()) {
			addSysproperty(String.valueOf(key), String.valueOf(getProject().getProperties().get(key)));
		}
		// java.addSyspropertyset(...)
	}

	@Override
	public void execute() throws BuildException {
		Java java = getJava();
		setup();
		java.setClassname(CLASSNAME);
		if(getClasspath() != null) {
			java.setClasspath(getClasspath());
		}
		if(getClasspathRef() != null) {
			java.setClasspathRef(classpathref);
		}
		// java.setFork(true);
		if(failonerror != null) {
			java.setFailonerror(failonerror);
		}
		java.execute();
	}
	
	void addSysproperty(String key, String value) {
		Environment.Variable v = new Variable();
		v.setKey(key);
		v.setValue(value);
		java.addSysproperty(v);
	}

	public Path getClasspath() {
		return classpath;
	}

	public void setClasspath(Path classpath) {
		this.classpath = classpath;
	}
	
	public void setClasspathRef(Reference classpathref) {
		this.classpathref = classpathref;
	}

	public Reference getClasspathRef() {
		return classpathref;
	}

	public void setFailonerror(boolean failonerror) {
		this.failonerror = failonerror;
	}

	public Commandline.Argument createArg() {
		return getJava().getCommandLine().createArgument();
	}

}
