package tbrugz.sqldump.sqlrun;

import java.sql.Connection;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.sqlrun.SQLRun.CommitStrategy;

public interface Executor {
	void setExecId(String execId);
	void setProperties(Properties prop);
	void setConnection(Connection conn);
	void setCommitStrategy(CommitStrategy commitStrategy);
	//XXX void execute();
	//XXX String getExecSuffix();
	List<String> getAuxSuffixes();
}
