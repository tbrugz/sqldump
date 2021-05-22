package tbrugz.sqldump.sqlrun.def;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

public interface Importer extends Executor {

	public long importData() throws SQLException, InterruptedException, IOException;

	public long importStream(InputStream is) throws SQLException, InterruptedException, IOException;
	
}
