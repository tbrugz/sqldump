package tbrugz.sqldump.dbmsfeatures;

public class MSSQLServerFeatures extends InformationSchemaFeatures {

	/*
	 * http://social.msdn.microsoft.com/Forums/sqlserver/en-US/0ba50e13-84cb-47b3-b047-2afcad3b4fa2/trigger-definition-from-system-tables
	 * http://stackoverflow.com/questions/636452/what-is-the-best-way-to-check-whether-a-trigger-exists-in-sql-server
	 * select name, object_definition(object_id) from sys.triggers ...
	 * 
	 * sys.triggers: http://msdn.microsoft.com/en-us/library/ms188746.aspx
	 */
	//XXX: add event_manipulation value ; also add action_orientation, action_timing, action_condition?
	@Override
	String grabDBTriggersQuery(String schemaPattern) {
		return "select null as trigger_catalog, s.name trigger_schema, t.name trigger_name, "+
			" '' as event_manipulation, ps.name as event_object_schema, p.name as event_object_table, "+
			" object_definition(t.object_id) action_statement, "+
			" null as action_orientation, null as action_timing, null as action_condition "+
			"\nfrom sys.triggers t "+
			"\ninner join sys.all_objects p on t.parent_id = p.object_id "+
			"\ninner join sys.all_objects ao on t.object_id = ao.object_id "+
			"\ninner join sys.schemas s ON s.schema_id = ao.schema_id "+
			"\ninner join sys.schemas ps ON ps.schema_id = p.schema_id "+
			"\nwhere s.name = '"+schemaPattern+"'";
	}
	
}
