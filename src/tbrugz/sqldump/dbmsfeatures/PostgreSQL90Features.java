package tbrugz.sqldump.dbmsfeatures;

public class PostgreSQL90Features extends PostgreSQLFeatures {

	@Override
	String grabDBTriggersQuery(String schemaPattern) {
		return "select trigger_catalog, trigger_schema, trigger_name, event_manipulation, event_object_schema, event_object_table, action_statement, action_orientation, condition_timing "
			+"from information_schema.triggers "
			+"order by trigger_catalog, trigger_schema, trigger_name, event_manipulation ";
	}

}
