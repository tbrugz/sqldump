package tbrugz.sqldump.dbmsfeatures;

/**
 * Uses (mostly) <code>DBMS_METADATA.GET_DDL()</code> function to grab schema data.
 * <code>SELECT_CATALOG_ROLE</code> privilege needed.
 * see also: http://stackoverflow.com/questions/116522/what-oracle-privileges-do-i-need-to-use-dbms-metadata-get-ddl
 */
public class OracleDbmsMetadataFeatures extends OracleFeatures {
	
	@Override
	String grabDBViewsQuery(String schemaPattern, String viewNamePattern) {
		return "SELECT owner, VIEW_NAME, 'VIEW' as view_type, "
				+ "DBMS_METADATA.GET_DDL('VIEW', VIEW_NAME, '"+schemaPattern+"') as TEXT "
				+ "FROM ALL_VIEWS "
				+ "where owner = '"+schemaPattern+"' "
				+ (viewNamePattern!=null?"and VIEW_NAME = '"+viewNamePattern+"' ":"")
				+ "ORDER BY VIEW_NAME";
	}
	
	@Override
	String grabDBMaterializedViewsQuery(String schemaPattern, String viewNamePattern) {
		return "select owner, mview_name, 'MATERIALIZED_VIEW' AS VIEW_TYPE, "
				+ "DBMS_METADATA.GET_DDL('MATERIALIZED_VIEW', mview_name, '"+schemaPattern+"') as query, "
				+" rewrite_enabled, rewrite_capability, refresh_mode, refresh_method, build_mode, fast_refreshable "
				+" from all_mviews "
				+" where owner = '"+schemaPattern+"'"
				+ (viewNamePattern!=null?"and mview_name = '"+viewNamePattern+"' ":"")
				+"ORDER BY MVIEW_NAME";
	}
	
	@Override
	String grabDBTriggersQuery(String schemaPattern, String tableNamePattern, String triggerNamePattern) {
		return "SELECT TRIGGER_NAME, TABLE_OWNER, TABLE_NAME, DESCRIPTION, "
				+"DBMS_METADATA.GET_DDL('TRIGGER', TRIGGER_NAME, '"+schemaPattern+"') as TRIGGER_BODY, "
				+"WHEN_CLAUSE "
				+"FROM ALL_TRIGGERS "
				+"where owner = '"+schemaPattern+"' "
				+(tableNamePattern!=null?" and table_name = '"+tableNamePattern+"' ":"")
				+(triggerNamePattern!=null?" and trigger_name = '"+triggerNamePattern+"' ":"")
				//+"and status = 'ENABLED' "
				+"ORDER BY trigger_name";
	}
	
	//XXX takes too long?
	/*
	@Override
	String grabDBExecutablesQuery(String schemaPattern) {
		return "select name, type, line, "
				+"DBMS_METADATA.GET_DDL(REPLACE(type, ' ', '_'), name, '"+schemaPattern+"') text "
				+"from all_source "
				+"where type in ('PROCEDURE','PACKAGE','PACKAGE BODY','FUNCTION','TYPE') "
				+"and owner = '"+schemaPattern+"' "
				+"order by type, name, line";
	}
	*/
	
	//XXX synonyms? sequences? indexes?
	
}
