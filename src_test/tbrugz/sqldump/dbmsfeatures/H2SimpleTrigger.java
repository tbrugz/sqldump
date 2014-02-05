package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.api.Trigger;

public class H2SimpleTrigger implements Trigger {

	@Override
	public void init(Connection conn, String schemaName, String triggerName,
			String tableName, boolean before, int type) throws SQLException {
		//called by the database engine once when initializing the trigger. It is called when the trigger is created, as well as when the database is opened
		System.out.println(H2SimpleTrigger.class.getName()+": init()");
	}

	@Override
	public void fire(Connection conn, Object[] oldRow, Object[] newRow)
			throws SQLException {
		// called for each triggered action
		System.out.println(H2SimpleTrigger.class.getName()+": fire()");
		if(newRow!=null && newRow.length>0 && newRow[0].equals("ERROR")) {
			throw new SQLException("'Error' parameter detected");
		}
	}

	@Override
	public void close() throws SQLException {
		//called when the database is closed
	}

	@Override
	public void remove() throws SQLException {
		//called when the trigger is dropped
	}

}
