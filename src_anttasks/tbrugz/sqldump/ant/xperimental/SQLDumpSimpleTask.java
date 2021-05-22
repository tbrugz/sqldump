package tbrugz.sqldump.ant.xperimental;

import java.util.Hashtable;
import java.util.Properties;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.PropertySet;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.def.Executor;

//TODO: add classpath!
public class SQLDumpSimpleTask extends Task {

	static final String[] NULL_PARAMS = {};
	
	String[] args = NULL_PARAMS;
	Boolean failonerror = null;
	
	public void setArgs(String[] args) {
		this.args = args;
	}
	
	public void setFailonerror(boolean failonerror) {
		this.failonerror = failonerror;
	}
	
	public void addPropertyset(PropertySet sysp) {
		//getCommandLine().addSyspropertyset(sysp);
	}

	@SuppressWarnings({ "rawtypes" })
	public void execute() throws BuildException {
		Executor sqldump = new SQLDump();
		try {
			if(failonerror!=null) {
				sqldump.setFailOnError(failonerror);
			}
			Properties p = new Properties();
			Hashtable ht = getProject().getProperties();
			p.putAll(ht);
			//debug(p);
			//XXX: does not understand @includes ?
			sqldump.doMain(args, p);
		} catch (Exception e) {
			e.printStackTrace();
			getProject().log("Error executing sqldump", Project.MSG_ERR);
			throw new BuildException("Error executing sqldump", e);
		}
	}
	
	/*void debug(Properties p) {
		for(Object o: p.keySet()) {
			getProject().log(o+": "+p.get(o), Project.MSG_INFO);
		}
	}*/

}
