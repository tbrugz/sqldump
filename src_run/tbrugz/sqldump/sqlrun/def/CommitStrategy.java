package tbrugz.sqldump.sqlrun.def;

public enum CommitStrategy {
	AUTO_COMMIT,
	//STATEMENT, //not implemented yet
	FILE,
	EXEC_ID,
	RUN,
	NONE
}