package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import tbrugz.sqldump.AbstractDatabaseMetaDataDecorator;

public class OracleDatabaseMetaData extends AbstractDatabaseMetaDataDecorator {
	
	static Logger log = Logger.getLogger(OracleDatabaseMetaData.class);
	
	public OracleDatabaseMetaData(DatabaseMetaData metadata) {
		this.metadata = metadata;
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
		
		Connection conn = metadata.getConnection();
		String sql = "select * from (";
		sql += "select '' as TABLE_CAT, owner as TABLE_SCHEM, TABLE_NAME, 'TABLE' as TABLE_TYPE, null as REMARKS, " 
				+"TABLESPACE_NAME, decode(TEMPORARY,'N','NO','Y','YES',null) as TEMPORARY, LOGGING, NUM_ROWS, BLOCKS "
				+"from all_tables ";
		//synonyms
		sql += "union select '' as TABLE_CAT, owner as TABLE_SCHEM, SYNONYM_NAME as TABLE_NAME, 'SYNONYM' as TABLE_TYPE, null as REMARKS, " 
				+"null as TABLESPACE_NAME, null as TEMPORARY, null as LOGGING, null as NUM_ROWS, null as BLOCKS "
				+"from all_synonyms ";
		//views
		sql += "union select '' as TABLE_CAT, owner as TABLE_SCHEM, VIEW_NAME as TABLE_NAME, 'VIEW' as TABLE_TYPE, null as REMARKS, " 
				+"null as TABLESPACE_NAME, null as TEMPORARY, null as LOGGING, null as NUM_ROWS, null as BLOCKS "
				+"from all_views ";
		sql += ") ";
		if(schemaPattern!=null) {
			sql += "where TABLE_SCHEM = '"+schemaPattern+"' ";
		}
		if(tableNamePattern!=null) {
			if(schemaPattern!=null) {
				sql += "and ";
			}
			else {
				sql += "where ";
			}
			sql += " TABLE_NAME = '"+tableNamePattern+"' ";
		}
		Statement st = conn.createStatement();
		log.debug("sql:\n"+sql);
		return st.executeQuery(sql);
	}
}
