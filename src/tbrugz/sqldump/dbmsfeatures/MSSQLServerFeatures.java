package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.SchemaModel;

public class MSSQLServerFeatures extends InformationSchemaFeatures {
	static final Log log = LogFactory.getLog(MSSQLServerFeatures.class);

	@Override
	void grabDBTriggers(SchemaModel model, String schemaPattern, Connection conn)
			throws SQLException {
		log.warn("grabTriggers: not implemented");
	}
}
