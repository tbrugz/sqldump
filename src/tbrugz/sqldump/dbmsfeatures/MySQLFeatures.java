package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.SQLException;

import tbrugz.sqldump.dbmodel.SchemaModel;

public class MySQLFeatures extends InformationSchemaFeatures {

	@Override
	String grabDBRoutinesQuery(String schemaPattern) {
		return "select routine_name, routine_type, '' as data_type, external_language, routine_definition "
				+"from information_schema.routines "
				+"where routine_definition is not null "
				+"order by routine_catalog, routine_schema, routine_name ";
	}
	
	@Override
	void grabDBSequences(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
	}
	
	/*
	 * check constraints seems not to be supported by mysql
	 * suggestion: using before-triggers: http://forums.mysql.com/read.php?136,152474,240479#msg-240479
	 */
	@Override
	void grabDBCheckConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
	}
	
	//XXX: see in information_schema: referential_constraints ; table_constraints -> PK, UNIQUE
	/*
	CREATE TEMPORARY TABLE `TABLE_CONSTRAINTS` (
		`CONSTRAINT_CATALOG` varchar(512) DEFAULT NULL,
		`CONSTRAINT_SCHEMA` varchar(64) NOT NULL DEFAULT '',
		`CONSTRAINT_NAME` varchar(64) NOT NULL DEFAULT '',
		`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT '',
		`TABLE_NAME` varchar(64) NOT NULL DEFAULT '',
		`CONSTRAINT_TYPE` varchar(64) NOT NULL DEFAULT ''
	) ENGINE=MEMORY DEFAULT CHARSET=utf8
	 */
	@Override
	String grabDBUniqueConstraintsQuery(String schemaPattern) {
		return "select tc.constraint_schema, tc.table_name, tc.constraint_name, column_name "
			+"from information_schema.table_constraints tc, information_schema.key_column_usage ccu "
			+"where tc.constraint_name = ccu.constraint_name "
			+"and tc.table_name = ccu.table_name " 
			+"and constraint_type = 'UNIQUE' "
			+"order by table_name, constraint_name, column_name";
	}
}
