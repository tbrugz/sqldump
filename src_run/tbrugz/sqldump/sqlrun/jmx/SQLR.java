package tbrugz.sqldump.sqlrun.jmx;

import java.lang.management.ManagementFactory;
import java.security.AccessControlException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

public class SQLR implements SQLRMBean {
	
	static final String MBEAN_NAME = "tbrugz.sqlrun:type=SQLRun";
	
	public SQLR(int maxPosition, DatabaseMetaData dbmd) {
		this.maxPosition = maxPosition;
		this.dbmd = dbmd;
		this.startTimeMilis = System.currentTimeMillis();
	}
	
	final int maxPosition;
	final long startTimeMilis;
	final DatabaseMetaData dbmd;
	
	int currentPosition;
	long currentTaskStartMilis;
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
		return ((double)currentPosition)/((double)maxPosition);
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
		return (System.currentTimeMillis()-startTimeMilis)/1000;
	}

	@Override
	public long getCurrentTaskElapsedSeconds() {
		return (System.currentTimeMillis()-currentTaskStartMilis)/1000;
	}

	public void newTaskUpdate(int currentPosition, String currentId, String currentTaskType, String currentTaskDefinition) {
		this.currentPosition = currentPosition;
		this.currentId = currentId;
		this.currentTaskType = currentTaskType;
		this.currentTaskDefinition = currentTaskDefinition;
		this.currentTaskStartMilis = System.currentTimeMillis();
	}
	
	public static void registerMBean(SQLRMBean mbean) throws MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		ObjectName name = new ObjectName(MBEAN_NAME);
		mbs.registerMBean(mbean, name);
	}

	public static void registerMBeanSimple(SQLRMBean mbean) {
		//TODO log exceptions
		try {
			registerMBean(mbean);
		} catch (MalformedObjectNameException e) {
			e.printStackTrace();
		} catch (InstanceAlreadyExistsException e) {
			e.printStackTrace();
		} catch (MBeanRegistrationException e) {
			e.printStackTrace();
		} catch (NotCompliantMBeanException e) {
			e.printStackTrace();
		} catch (AccessControlException e) {
			e.printStackTrace();
		}
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
