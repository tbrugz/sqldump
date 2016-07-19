package tbrugz.sqldump.sqlrun.def;

import java.io.IOException;
import java.sql.SQLException;

public interface Importer extends Executor {

	public long importData() throws SQLException, InterruptedException, IOException;
	
}
