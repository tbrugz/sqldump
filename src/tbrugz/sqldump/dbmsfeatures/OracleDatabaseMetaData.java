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
		sql += "order by TABLE_SCHEM, TABLE_NAME";
		Statement st = conn.createStatement();
		log.debug("sql:\n"+sql);
		return st.executeQuery(sql);
	}
	
	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException {
		Connection conn = metadata.getConnection();
		String sql = "select * from (";
		sql += "select '' as TABLE_CAT, col.owner as TABLE_SCHEM, col.TABLE_NAME, col.COLUMN_NAME, data_type as TYPE_NAME, "
				+"nvl(data_precision, data_length) as COLUMN_SIZE, data_scale as DECIMAL_DIGITS, decode(NULLABLE, 'Y', 'YES', 'N', 'NO', null) as IS_NULLABLE, "
				+"DATA_DEFAULT, COLUMN_ID as ORDINAL_POSITION, comments "
				+"from all_tab_columns col, all_col_comments com "
				+"where col.column_name = com.column_name and col.table_name = com.table_name and col.owner = com.owner "
				+") ";
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
		sql += "order by TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION ";
		Statement st = conn.createStatement();
		log.debug("sql:\n"+sql);
		return st.executeQuery(sql);
	}
}
