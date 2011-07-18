package tbrugz.sqldump;

import java.util.Properties;

public interface SchemaModelGrabber {

	public void procProperties(Properties prop);
	
	public SchemaModel grabSchema() throws Exception;

}