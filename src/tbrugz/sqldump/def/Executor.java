package tbrugz.sqldump.def;

import java.util.Properties;

//XXX: rename? package 'tbrugz.sqldump.sqlrun.def' has class of same name
public interface Executor {
	
	public void doMain(String[] args, Properties prop) throws Exception;
	public void setFailOnError(boolean failonerror);
	
}
