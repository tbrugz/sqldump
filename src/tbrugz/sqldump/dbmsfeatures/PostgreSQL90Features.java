package tbrugz.sqldump.dbmsfeatures;

public class PostgreSQL90Features extends PostgreSQLAbstractFeatutres {

	/*
	 * see: http://www.postgresql.org/docs/8.1/static/infoschema-triggers.html
	 */
	@Override
	String grabDBTriggersQuery(String schemaPattern) {
		return "select trigger_catalog, trigger_schema, trigger_name, event_manipulation, event_object_schema, event_object_table, action_statement, action_orientation, condition_timing, null as action_condition "
			+"from information_schema.triggers "
			+"where trigger_schema = '"+schemaPattern+"' "
			+"order by trigger_catalog, trigger_schema, trigger_name, event_manipulation ";
	}

}
