package tbrugz.sqldump.def;

import java.util.Properties;

public interface Executor {
	
	public void doMain(String[] args, Properties prop) throws Exception;
	
}
