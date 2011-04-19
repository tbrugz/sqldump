package tbrugz.sqldump;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public interface DBMSFeatures {
	static String PROP_GRAB_INDEXES = "sqldump.dbspecificfeatures.grabindexes";
	static String PROP_SEQUENCE_STARTWITHDUMP = "sqldump.dbspecificfeatures.sequencestartwithdump";
	
	void procProperties(Properties prop);
	void grabDBObjects(SchemaModel model, String schemaPattern,	Connection conn) throws SQLException;
}