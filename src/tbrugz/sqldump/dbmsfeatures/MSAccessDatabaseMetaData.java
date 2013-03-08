package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.AbstractDatabaseMetaDataDecorator;

public class MSAccessDatabaseMetaData extends AbstractDatabaseMetaDataDecorator {

	static Log log = LogFactory.getLog(MSAccessDatabaseMetaData.class);

	public MSAccessDatabaseMetaData(DatabaseMetaData metadata) {
		this.metadata = metadata;
	}
	
	@Override
	public ResultSet getSchemas() throws SQLException {
		return null;
	}
	
	@Override
	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		Connection conn = metadata.getConnection();
		
		String sql = "select null as TABLE_CAT, null as TABLE_SCHEM, name as TABLE_NAME, '' as REMARKS, "
				+"iif([Flags] = 0, 'TABLE', 'SYSTEM TABLE') as TABLE_TYPE\n"
				//+"[Type] as TABLE_TYPE\n"
				+"from [MSysObjects] "
				+"where [Type] = 1 "
				+"order by [Type], [Name]";
		PreparedStatement st = conn.prepareStatement(sql);
		log.debug("sql:\n"+sql);
		return st.executeQuery();
	}
	
	@Override
	public ResultSet getColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		log.info("getColumns() from: "+tableNamePattern); 
		//return null;
		return super.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}
	
	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException {
		//return null;
		return super.getPrimaryKeys(catalog, schema, table);
	}
}
