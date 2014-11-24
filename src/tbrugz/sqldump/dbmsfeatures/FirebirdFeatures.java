package tbrugz.sqldump.dbmsfeatures;

import java.util.Properties;

public class FirebirdFeatures extends InformationSchemaFeatures {

	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		
		grabExecutables = false; //XXX: grab executables?
		grabCheckConstraints = false; //XXX: grab check constraints?
	}
	
	@Override
	String grabDBViewsQuery(String schemaPattern) {
		return "select '' as table_catalog, '' as table_schema, trim(RDB$RELATION_NAME) as table_name, "
			+"RDB$VIEW_SOURCE as view_definition, 'NONE' as check_option, '' as is_updatable "
			+"from RDB$RELATIONS "
			+"where RDB$VIEW_SOURCE is not null "
			+"order by RDB$RELATION_NAME ";
	}
	
	@Override
	String grabDBTriggersQuery(String schemaPattern) {
		return "select '' as trigger_catalog, '' as trigger_schema, RDB$TRIGGER_NAME AS trigger_name, "
			+"CASE RDB$TRIGGER_TYPE WHEN 1 THEN 'INSERT' WHEN 2 THEN 'INSERT' WHEN 3 THEN 'UPDATE' WHEN 4 THEN 'UPDATE' WHEN 5 THEN 'DELETE' WHEN 6 THEN 'DELETE' end as event_manipulation, "
			+"'' as event_object_schema, RDB$RELATION_NAME as event_object_table, " 
			+"RDB$TRIGGER_SOURCE AS action_statement, "
			+"null as action_orientation, "
			+"CASE RDB$TRIGGER_TYPE "
			+"WHEN 1 THEN 'BEFORE' "
			+"WHEN 2 THEN 'AFTER' "
			+"WHEN 3 THEN 'BEFORE' "
			+"WHEN 4 THEN 'AFTER' "
			+"WHEN 5 THEN 'BEFORE' "
			+"WHEN 6 THEN 'AFTER' "
			+"END AS action_timing, "
			+"null as action_condition "
			//"CASE RDB$TRIGGER_INACTIVE WHEN 1 THEN 0 ELSE 1 END AS trigger_enabled,"
			//"RDB$DESCRIPTION AS trigger_comment"
			+"FROM RDB$TRIGGERS "
			+"where RDB$TRIGGER_SOURCE is not null";
	}
	
	@Override
	String grabDBSequencesQuery(String schemaPattern) {
		return "select trim(RDB$GENERATOR_NAME), 0 as minimum_value, 1 as increment, null as maximum_value "
				+"FROM RDB$GENERATORS "
				+"WHERE RDB$SYSTEM_FLAG=0 ";
 				//RDB$DESCRIPTION ?
	}
	
	@Override
	String grabDBUniqueConstraintsQuery(String schemaPattern, String constraintNamePattern) {
		return "select null as constraint_schema, trim(i.RDB$RELATION_NAME) as table_name, trim(rc.RDB$CONSTRAINT_NAME) as constraint_name, trim(s.RDB$FIELD_NAME) as column_name, "
				+"s.RDB$FIELD_POSITION as column_position "
				+"FROM RDB$INDEX_SEGMENTS s "
				+"LEFT JOIN RDB$INDICES i ON i.RDB$INDEX_NAME = s.RDB$INDEX_NAME "
				+"LEFT JOIN RDB$RELATION_CONSTRAINTS rc ON rc.RDB$INDEX_NAME = s.RDB$INDEX_NAME "
				+"WHERE rc.RDB$CONSTRAINT_TYPE IS NOT NULL "
				+"  AND trim(rc.RDB$CONSTRAINT_TYPE) = 'UNIQUE' "
				+(constraintNamePattern!=null?"  AND trim(rc.RDB$CONSTRAINT_NAME) = '"+constraintNamePattern+"' ":"")
				+"ORDER BY i.RDB$RELATION_NAME, rc.RDB$CONSTRAINT_NAME, s.RDB$FIELD_POSITION";
	}
	
}
