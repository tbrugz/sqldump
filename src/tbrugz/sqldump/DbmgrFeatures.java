package tbrugz.sqldump;

import java.sql.Connection;
import java.sql.SQLException;

public interface DbmgrFeatures {
	void grabDBObjects(SchemaModel model, String schemaPattern,	Connection conn) throws SQLException;
}