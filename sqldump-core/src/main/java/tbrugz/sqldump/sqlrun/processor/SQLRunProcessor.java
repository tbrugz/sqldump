package tbrugz.sqldump.sqlrun.processor;

import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.sqlrun.SQLRun;

public class SQLRunProcessor extends AbstractSQLProc {

	@Override
	public void process() {
		SQLRun sqlr = new SQLRun();
		try {
			sqlr.doMain(null, getProperties(), getConnection());
		} catch (Exception e) {
			throw new ProcessingException(e);
		}
	}
	
}
