package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.AbstractDatabaseMetaDataDecorator;
import tbrugz.sqldump.resultset.EmptyResultSet;

public class MSAccessDatabaseMetaData extends AbstractDatabaseMetaDataDecorator {

	static Log log = LogFactory.getLog(MSAccessDatabaseMetaData.class);

	public MSAccessDatabaseMetaData(DatabaseMetaData metadata) {
		this.metadata = metadata;
	}
	
	@Override
	public ResultSet getSchemas() throws SQLException {
		return null;
	}
	
	/*
	
	MSysObjects: type:
	1 - Table / System Table
	3? - system tables?
	5 - Query (View...)
	8 - Relationship
	-32768 - Form
	-32766 - Macro
	-32764 - Report
	-32761 - Module
	-32756 - Pages
	
	*/
	
	@Override
	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		Connection conn = metadata.getConnection();
		
		String sql = "select null as TABLE_CAT, null as TABLE_SCHEM, name as TABLE_NAME, '' as REMARKS, "
				+"iif([Type] = 5, 'VIEW', iif([Flags] = 0, 'TABLE', 'SYSTEM TABLE')) as TABLE_TYPE\n"
				//+"iif([Flags] = 0, 'TABLE', 'SYSTEM TABLE') as TABLE_TYPE\n"
				+"from [MSysObjects] "
				+"where [Type] in ( 1 , 5 ) "
				+"order by [Type], [Name]";
		PreparedStatement st = conn.prepareStatement(sql);
		log.debug("sql:\n"+sql);
		return st.executeQuery();
	}
	
	/*@Override
	public ResultSet getColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		//log.info("getColumns() from: "+tableNamePattern); 
		//return null;
		return super.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}*/
	
	//TODO: add getPrimaryKeys()
	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException {
		return new EmptyResultSet();
		//return null;
		//return super.getPrimaryKeys(catalog, schema, table);
	}

	//TODO: getImportedKeys() (& getExportedKeys()?)
	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {
		return new EmptyResultSet();
	}
	
	//TODO: getTablePrivileges()
	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		return new EmptyResultSet();
	}
}
