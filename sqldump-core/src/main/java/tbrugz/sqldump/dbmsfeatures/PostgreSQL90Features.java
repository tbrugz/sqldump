package tbrugz.sqldump.dbmsfeatures;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.dbmodel.QueryWithParams;

public class PostgreSQL90Features extends PostgreSQLAbstractFeatures {

	/*
	 * see: http://www.postgresql.org/docs/8.1/static/infoschema-triggers.html
	 */
	@Override
	QueryWithParams grabDBTriggersQuery(String schemaPattern, String tableNamePattern, String triggerNamePattern) {
		List<Object> params = new ArrayList<>();
		String query = "select trigger_catalog, trigger_schema, trigger_name, event_manipulation, event_object_schema, "
			+ "event_object_table, action_statement, action_orientation, condition_timing, null as action_condition "
			+ "from information_schema.triggers "
			+ "where trigger_schema = ? ";
		params.add(schemaPattern);
		if(tableNamePattern!=null) {
			query += "and event_object_table = ? ";
			params.add(tableNamePattern);
		}
		if(triggerNamePattern!=null) {
			query += "and trigger_name = ? ";
			params.add(triggerNamePattern);
		}
		query += "order by trigger_catalog, trigger_schema, trigger_name, event_manipulation ";
		return new QueryWithParams(query, params);
	}

}
