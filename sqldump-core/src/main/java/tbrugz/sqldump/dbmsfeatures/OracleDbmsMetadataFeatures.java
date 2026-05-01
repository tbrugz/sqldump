package tbrugz.sqldump.dbmsfeatures;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.dbmodel.QueryWithParams;

/**
 * Uses (mostly) <code>DBMS_METADATA.GET_DDL()</code> function to grab schema data.
 * <code>SELECT_CATALOG_ROLE</code> privilege needed.
 * see also: http://stackoverflow.com/questions/116522/what-oracle-privileges-do-i-need-to-use-dbms-metadata-get-ddl
 */
public class OracleDbmsMetadataFeatures extends OracleFeatures {
	
	@Override
	QueryWithParams grabDBViewsQuery(String schemaPattern, String viewNamePattern) {
		List<Object> params = new ArrayList<>();
		String query = "SELECT owner, VIEW_NAME, 'VIEW' as view_type, "
				+ "DBMS_METADATA.GET_DDL('VIEW', VIEW_NAME, ?) as TEXT "
				+ "FROM ALL_VIEWS "
				+ "where owner = ? ";
		params.add(schemaPattern);
		params.add(schemaPattern);
		if(viewNamePattern!=null) {
				query += "and VIEW_NAME = ? ";
				params.add(viewNamePattern);
		}
		query += "ORDER BY VIEW_NAME";
		return new QueryWithParams(query, params);
	}
	
	@Override
	QueryWithParams grabDBMaterializedViewsQuery(String schemaPattern, String viewNamePattern) {
		List<Object> params = new ArrayList<>();
		String query = "select owner, mview_name, 'MATERIALIZED_VIEW' AS VIEW_TYPE, "
				+ "DBMS_METADATA.GET_DDL('MATERIALIZED_VIEW', mview_name, ?) as query, "
				+" rewrite_enabled, rewrite_capability, refresh_mode, refresh_method, build_mode, fast_refreshable "
				+" from all_mviews "
				+" where owner = ? ";
		params.add(schemaPattern);
		params.add(schemaPattern);
		if(viewNamePattern!=null) {
				query += "and mview_name = ? ";
				params.add(viewNamePattern);
		}
		query += "ORDER BY MVIEW_NAME";
		return new QueryWithParams(query, params);
	}
	
	@Override
	QueryWithParams grabDBTriggersQuery(String schemaPattern, String tableNamePattern, String triggerNamePattern) {
		List<Object> params = new ArrayList<>();
		String query = "SELECT TRIGGER_NAME, TABLE_OWNER, TABLE_NAME, DESCRIPTION, "
				+"DBMS_METADATA.GET_DDL('TRIGGER', TRIGGER_NAME, ?) as TRIGGER_BODY, "
				+"WHEN_CLAUSE "
				+"FROM ALL_TRIGGERS "
				+"where owner = ? ";
		params.add(schemaPattern);
		params.add(schemaPattern);
		if(tableNamePattern!=null) {
				query += " and table_name = ? ";
				params.add(tableNamePattern);
		}
		if(triggerNamePattern!=null) {
				query += " and trigger_name = ? ";
				params.add(triggerNamePattern);
		}
				//+"and status = 'ENABLED' "
		query += "ORDER BY trigger_name";
		return new QueryWithParams(query, params);
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
