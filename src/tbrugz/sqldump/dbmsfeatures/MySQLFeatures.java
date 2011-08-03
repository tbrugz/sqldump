package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.SQLException;

import tbrugz.sqldump.SchemaModel;

public class MySQLFeatures extends InformationSchemaFeatures {

	@Override
	String grabDBRoutinesQuery() {
		return "select routine_name, routine_type, '' as data_type, external_language, routine_definition "
				+"from information_schema.routines "
				+"where routine_definition is not null "
				+"order by routine_catalog, routine_schema, routine_name ";
	}

	@Override
	String grabDBTriggersQuery() {
		return "select trigger_catalog, trigger_schema, trigger_name, event_manipulation, event_object_schema, event_object_table, action_statement, action_orientation, action_timing "
			+"from information_schema.triggers "
			+"order by trigger_catalog, trigger_schema, trigger_name, event_manipulation ";
	}
	
	@Override
	void grabDBSequences(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
	}
	
	//XXX: see in information_schema: referential_constraints ; table_constraints -> PK, UNIQUE
	@Override
	void grabDBConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
	}
}
