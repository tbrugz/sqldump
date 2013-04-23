package tbrugz.sqldump.sqlrun.jmx;

import java.sql.SQLException;

public interface SQLRMBean {

	String getName();
	
	int getMaxPosition();
	
	double getFinishedPercentage();
	
	long getTotalElapsedSeconds();
	
	String getConnectionUrl() throws SQLException;

	String getConnectionUsername() throws SQLException;
	
	String getCurrentId();
	
	int getCurrentPosition();

	String getCurrentTaskType();
	
	String getCurrentTaskDefinition();
	
	long getCurrentTaskElapsedSeconds();
}
