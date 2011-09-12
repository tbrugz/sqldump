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

		//REMARKS String => comment describing column (may be null)
		Connection conn = metadata.getConnection();
		String sql = "select tables.*, comm.comments as REMARKS from (\n";
		sql += "select '' as TABLE_CAT, owner as TABLE_SCHEM, TABLE_NAME, 'TABLE' as TABLE_TYPE, null as REMARKSz, " 
				+"TABLESPACE_NAME, decode(TEMPORARY,'N','NO','Y','YES',null) as TEMPORARY, LOGGING, NUM_ROWS, BLOCKS "
				+", owner as TABLE_SCHEM_FILTER "
				+"from all_tables where (owner, table_name) not in (select owner, mview_name from all_mviews union select owner, table_name from all_external_tables) \n";
		//synonyms
		sql += "union select '' as TABLE_CAT, allt.owner as TABLE_SCHEM, SYNONYM_NAME as TABLE_NAME, 'SYNONYM' as TABLE_TYPE, null as REMARKSz, " 
				+"null as TABLESPACE_NAME, null as TEMPORARY, null as LOGGING, null as NUM_ROWS, null as BLOCKS "
				//+"-- ,alls.owner as synonym_owner, allt.owner as table_owner \n" 
				+", alls.owner as TABLE_SCHEM_FILTER "
				+"from all_synonyms alls, all_tables allt "
				+"where alls.table_owner = allt.owner and alls.table_name = allt.table_name \n";
		//views
		sql += "union select '' as TABLE_CAT, owner as TABLE_SCHEM, VIEW_NAME as TABLE_NAME, 'VIEW' as TABLE_TYPE, null as REMARKSz, " 
				+"null as TABLESPACE_NAME, null as TEMPORARY, null as LOGGING, null as NUM_ROWS, null as BLOCKS "
				+", owner as TABLE_SCHEM_FILTER "
				+"from all_views \n";
		//materialized views
		sql += "union select '' as TABLE_CAT, allmv.owner as TABLE_SCHEM, MVIEW_NAME as TABLE_NAME, 'MATERIALIZED VIEW' as TABLE_TYPE, null as REMARKSz, "
				+"TABLESPACE_NAME, decode(TEMPORARY,'N','NO','Y','YES',null) as TEMPORARY, LOGGING, NUM_ROWS, BLOCKS, "
				+"allmv.owner as TABLE_SCHEM_FILTER "
				+"from all_tables allt, all_mviews allmv where allt.owner = allmv.owner and allt.table_name = allmv.mview_name \n";
		//external tables
		sql += "union select '' as TABLE_CAT, owner as TABLE_SCHEM, TABLE_NAME, 'EXTERNAL TABLE' as TABLE_TYPE, null as REMARKSz, "
				+"null as TABLESPACE_NAME, null as TEMPORARY, null as LOGGING, null as NUM_ROWS, null as BLOCKS, "
				+"owner as TABLE_SCHEM_FILTER "
				+"from all_external_tables \n";
		sql += ") tables, all_tab_comments comm \nwhere tables.TABLE_SCHEM = comm.owner (+) and tables.TABLE_NAME = comm.TABLE_NAME (+) ";
		if(schemaPattern!=null) {
			sql += "and TABLE_SCHEM_FILTER = '"+schemaPattern+"' ";
		}
		if(tableNamePattern!=null) {
			sql += "and tables.TABLE_NAME = '"+tableNamePattern+"' ";
		}
		sql += "order by tables.TABLE_SCHEM, tables.TABLE_NAME";
		Statement st = conn.createStatement();
		log.debug("sql:\n"+sql);
		return st.executeQuery(sql);
	}
	
	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException {
		Connection conn = metadata.getConnection();
		String sql = "select * from (";
		//strange: non-default columns must be at end (or 'sqlexception: already closed stream' can occur). maybe because DATA_DEFAULT is of type LONG/MEMO (read from a stream?)
		sql += "select '' as TABLE_CAT, col.owner as TABLE_SCHEM, col.TABLE_NAME, col.COLUMN_NAME, data_type as TYPE_NAME, "
				+"nvl(data_precision, data_length) as COLUMN_SIZE, data_scale as DECIMAL_DIGITS, decode(NULLABLE, 'Y', 'YES', 'N', 'NO', null) as IS_NULLABLE, "
				+"COLUMN_ID as ORDINAL_POSITION, comments as REMARKS, DATA_DEFAULT "
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
	
	static boolean grabFKFromUK = false;
	
	/**
	 * added a UK_CONSTRAINT_TYPE column, which returns: P - primary key, U - unique key
	 */
	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {
		//TODOne: do not grab FKs that do not reference a PK
		
		if(!grabFKFromUK) {
			return super.getImportedKeys(catalog, schema, table);
		}
		
		//XXX: this makes grabbing slower. prop option to use default method
		Connection conn = metadata.getConnection();
		String sql = "select * from (";
		sql += "select '' as PKTABLE_CAT, acuk.owner as PKTABLE_SCHEM, acuk.table_name as PKTABLE_NAME, accuk.column_name as PKCOLUMN_NAME, \n"
				+"       '' as FKTABLE_CAT, acfk.owner as FKTABLE_SCHEM, acfk.table_name as FKTABLE_NAME, accfk.column_name as FKCOLUMN_NAME, \n"
				+"       accuk.position as KEY_SEQ, '' as UPDATE_RULE, '' as DELETE_RULE, \n"
				+"       acfk.constraint_name as FK_NAME, acfk.r_constraint_name as PK_NAME, '' as DEFERRABILITY \n"
				+"       ,acuk.constraint_type as UK_CONSTRAINT_TYPE " //returns type of unique key: P - primary, U - unique
				+"from all_constraints acfk, all_cons_columns accfk, \n"
				+" all_constraints acuk, all_cons_columns accuk \n"
				+"where acfk.owner = accfk.owner and acfk.constraint_name = accfk.constraint_name and acfk.constraint_type = 'R' \n"
				+"  and acuk.owner = accuk.owner and acuk.constraint_name = accuk.constraint_name and acuk.constraint_type in ('P','U') \n"
				+"  and acfk.r_owner = acuk.owner and acfk.r_constraint_name = acuk.constraint_name \n"
				+"  and accfk.position = accuk.position \n" 
				+"order by acfk.owner, acfk.constraint_name, accfk.position "
				+") ";

		if(schema!=null) {
			sql += "where FKTABLE_SCHEM = '"+schema+"' \n";
		}
		if(table!=null) {
			if(schema!=null) {
				sql += "and ";
			}
			else {
				sql += "where ";
			}
			sql += "FKTABLE_NAME = '"+table+"' ";
		}
		sql += "order by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ ";
		Statement st = conn.createStatement();
		log.debug("sql:\n"+sql);
		return st.executeQuery(sql);
	}
	
}
