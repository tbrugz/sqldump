package tbrugz.sqldump.sqlrun.def;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

public interface Importer extends Executor {

	public long importData() throws SQLException, IOException;

	public long importStream(InputStream is) throws SQLException, IOException;
	
	public long importFilesGlob(String filesGlobPattern, File importDir) throws SQLException, IOException;

}
