package tbrugz.sqldump.sqlrun.jmx;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class SQLR implements SQLRMBean {
	
	public static final String MBEAN_NAME = "tbrugz.sqlrun:type=SQLRun";
	
	public SQLR(int maxPosition, DatabaseMetaData dbmd) {
		this.maxPosition = maxPosition;
		this.dbmd = dbmd;
		this.startTimeMillis = System.currentTimeMillis();
	}
	
	final int maxPosition;
	final long startTimeMillis;
	protected DatabaseMetaData dbmd;
	
	int currentPosition;
	long currentTaskStartMillis;
	String currentId;
	String currentTaskType;
	String currentTaskDefinition;

	@Override
	public String getName() {
		return "SQLRun MBean";
	}

	@Override
	public int getCurrentPosition() {
		return currentPosition;
	}

	@Override
	public int getMaxPosition() {
		return maxPosition;
	}
	
	@Override
	public double getFinishedPercentage() {
		return ((double)currentPosition-1)/((double)maxPosition);
	}

	@Override
	public String getCurrentId() {
		return currentId;
	}

	@Override
	public String getCurrentTaskType() {
		return currentTaskType;
	}
	
	@Override
	public String getCurrentTaskDefinition() {
		return currentTaskDefinition;
	}

	@Override
	public long getTotalElapsedSeconds() {
		return (System.currentTimeMillis()-startTimeMillis)/1000;
	}

	@Override
	public long getCurrentTaskElapsedSeconds() {
		return (System.currentTimeMillis()-currentTaskStartMillis)/1000;
	}

	public void newTaskUpdate(int currentPosition, String currentId, String currentTaskType, String currentTaskDefinition) {
		this.currentPosition = currentPosition;
		this.currentId = currentId;
		this.currentTaskType = currentTaskType;
		this.currentTaskDefinition = currentTaskDefinition;
		this.currentTaskStartMillis = System.currentTimeMillis();
	}

	@Override
	public String getConnectionUrl() throws SQLException {
		return dbmd.getURL();
	}
	
	@Override
	public String getConnectionUsername() throws SQLException {
		return dbmd.getUserName();
	}
	
}
