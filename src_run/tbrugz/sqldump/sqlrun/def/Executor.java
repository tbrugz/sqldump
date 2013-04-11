package tbrugz.sqldump.sqlrun.def;

import java.sql.Connection;
import java.util.List;
import java.util.Properties;

public interface Executor {
	void setExecId(String execId);
	void setProperties(Properties prop);
	void setConnection(Connection conn);
	void setCommitStrategy(CommitStrategy commitStrategy);
	void setFailOnError(boolean failonerror);
	//XXX void execute();
	List<String> getExecSuffixes();
	List<String> getAuxSuffixes();
}
