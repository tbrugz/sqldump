package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.AbstractDatabaseMetaDataDecorator;

public class OracleDatabaseMetaData extends AbstractDatabaseMetaDataDecorator {
	
	static Log log = LogFactory.getLog(OracleDatabaseMetaData.class);

	boolean useDbaMetadataObjects = OracleFeatures.useDbaMetadataObjects;
	
	public OracleDatabaseMetaData(DatabaseMetaData metadata) {
		super(metadata);
	}

	/**
	 * @param types avaiable table types: TABLE, SYNONYM, VIEW, MATERIALIZED VIEW, EXTERNAL TABLE
	 */
	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
		//REMARKS String => comment describing column (may be null)
		
		// only tables & materialized views may have comments/remarks
		// http://docs.oracle.com/cd/B19306_01/server.102/b14200/statements_4009.htm
		
		//log.info("getTables: types="+Arrays.toString(types));
		
		List<String> ltypes = null;
		if(types!=null) { ltypes = Arrays.asList(types); };
		boolean firstTypeAdded = false;
		
		Connection conn = metadata.getConnection();
		List<String> params = new ArrayList<String>();
		
		StringBuilder sql = new StringBuilder();
				sql.append("select tables.* from (\n");
		//tables
		if(ltypes==null || ltypes.contains("TABLE")) {
			sql.append("select '' as TABLE_CAT, at.owner as TABLE_SCHEM, at.TABLE_NAME, 'TABLE' as TABLE_TYPE, comm.comments as REMARKS, " 
				+"TABLESPACE_NAME, decode(TEMPORARY,'N','NO','Y','YES',null) as TEMPORARY, LOGGING, NUM_ROWS, BLOCKS, "
				+"PARTITIONED, PARTITIONING_TYPE, at.owner as TABLE_SCHEM_FILTER\n"
				+(useDbaMetadataObjects?
					"from dba_tables at, dba_part_tables apt, dba_tab_comments comm\n":
					"from all_tables at, all_part_tables apt, all_tab_comments comm\n"
				)
				+"where at.owner = apt.owner (+) and at.table_name = apt.table_name (+) and at.owner = comm.owner (+) and at.table_name = comm.TABLE_NAME (+) "
				+"and (at.owner, at.table_name) not in (select owner, mview_name from all_mviews union select owner, table_name from all_external_tables) \n");
			firstTypeAdded = true;
		}
		//synonyms
		if(ltypes==null || ltypes.contains("SYNONYM")) {
			if(firstTypeAdded) { sql.append("union\n"); } else { firstTypeAdded = true; }
			sql.append("select '' as TABLE_CAT, allt.owner as TABLE_SCHEM, SYNONYM_NAME as TABLE_NAME, 'SYNONYM' as TABLE_TYPE, null as REMARKS, " 
				+"null as TABLESPACE_NAME, null as TEMPORARY, null as LOGGING, null as NUM_ROWS, null as BLOCKS, null as PARTITIONED, null as PARTITIONING_TYPE, "
				//+"-- ,alls.owner as synonym_owner, allt.owner as table_owner \n" 
				+"alls.owner as TABLE_SCHEM_FILTER "
				+(useDbaMetadataObjects?
						"from dba_synonyms alls, dba_tables allt ":
						"from all_synonyms alls, all_tables allt "
					)
				+"where alls.table_owner = allt.owner and alls.table_name = allt.table_name \n");
		}
		//views
		if(ltypes==null || ltypes.contains("VIEW")) {
			if(firstTypeAdded) { sql.append("union\n"); } else { firstTypeAdded = true; }
			sql.append("select '' as TABLE_CAT, owner as TABLE_SCHEM, VIEW_NAME as TABLE_NAME, 'VIEW' as TABLE_TYPE, null as REMARKS, " 
				+"null as TABLESPACE_NAME, null as TEMPORARY, null as LOGGING, null as NUM_ROWS, null as BLOCKS, null as PARTITIONED, null as PARTITIONING_TYPE, "
				+"owner as TABLE_SCHEM_FILTER "
				+"from "
				+(useDbaMetadataObjects?"dba_views \n":"all_views \n")
				);
		}
		//materialized views
		if(ltypes==null || ltypes.contains("MATERIALIZED VIEW")) {
			if(firstTypeAdded) { sql.append("union\n"); } else { firstTypeAdded = true; }
			sql.append("select '' as TABLE_CAT, allmv.owner as TABLE_SCHEM, allmv.MVIEW_NAME as TABLE_NAME, 'MATERIALIZED VIEW' as TABLE_TYPE, mvcomm.comments as REMARKS, "
				+"TABLESPACE_NAME, decode(TEMPORARY,'N','NO','Y','YES',null) as TEMPORARY, LOGGING, NUM_ROWS, BLOCKS, null as PARTITIONED, null as PARTITIONING_TYPE, "
				+"allmv.owner as TABLE_SCHEM_FILTER "
				+(useDbaMetadataObjects?
					"from dba_tables allt, dba_mviews allmv, dba_mview_comments mvcomm ":
					"from all_tables allt, all_mviews allmv, all_mview_comments mvcomm "
				)
				+"where allt.owner = allmv.owner and allt.table_name = allmv.mview_name and allt.owner = mvcomm.owner and allt.table_name = mvcomm.mview_name ");
		}
		//external tables
		if(ltypes==null || ltypes.contains("EXTERNAL TABLE")) {
			if(firstTypeAdded) { sql.append("union\n"); } else { firstTypeAdded = true; }
			sql.append("select '' as TABLE_CAT, owner as TABLE_SCHEM, TABLE_NAME, 'EXTERNAL TABLE' as TABLE_TYPE, null as REMARKS, "
				+"null as TABLESPACE_NAME, null as TEMPORARY, null as LOGGING, null as NUM_ROWS, null as BLOCKS, null as PARTITIONED, null as PARTITIONING_TYPE, "
				+"owner as TABLE_SCHEM_FILTER "
				+"from "
				+(useDbaMetadataObjects?"dba_external_tables \n":"all_external_tables \n")
				);
		}
		if(!firstTypeAdded) {
			throw new SQLException("not one table type valid: "+ltypes);
		}
		sql.append(
			") tables \n"
			+ "where 1=1 ");
			//+ ", all_tab_comments comm \nwhere tables.TABLE_SCHEM = comm.owner (+) and tables.TABLE_NAME = comm.TABLE_NAME (+) ";
			//+ "\n left outer join all_tab_comments comm on tables.TABLE_SCHEM = comm.owner and tables.TABLE_NAME = comm.TABLE_NAME "
			//+ "\n left outer join all_mview_comments mvcomm on tables.TABLE_SCHEM = mvcomm.owner and tables.TABLE_NAME = mvcomm.mview_name ";
		if(schemaPattern!=null) {
			sql.append("and TABLE_SCHEM_FILTER like ? ");
			params.add(schemaPattern);
		}
		if(tableNamePattern!=null) {
			sql.append("and tables.TABLE_NAME like ? ");
			params.add(tableNamePattern);
		}
		sql.append("order by tables.TABLE_SCHEM, tables.TABLE_NAME");
		
		PreparedStatement st = conn.prepareStatement(sql.toString());
		for(int i=0;i<params.size();i++) {
			st.setString(i+1, params.get(i));
		}
		log.debug("sql:\n"+sql);
		return st.executeQuery();
	}
	
	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException {
		Connection conn = metadata.getConnection();
		List<String> params = new ArrayList<String>();
		
		String sql = "select * from (";
		//strange: non-default columns must be at end (or 'sqlexception: already closed stream' can occur). maybe because DATA_DEFAULT is of type LONG/MEMO (read from a stream?)
		sql += "select '' as TABLE_CAT, col.owner as TABLE_SCHEM, col.TABLE_NAME, col.COLUMN_NAME, data_type as TYPE_NAME, "
				+"nvl(data_precision, data_length) as COLUMN_SIZE, data_scale as DECIMAL_DIGITS, decode(NULLABLE, 'Y', 'YES', 'N', 'NO', null) as IS_NULLABLE, "
				+"COLUMN_ID as ORDINAL_POSITION, comments as REMARKS, DATA_DEFAULT "
				+"from "+(useDbaMetadataObjects?"dba_tab_columns col, dba_col_comments com ":"all_tab_columns col, all_col_comments com ")
				+"where col.column_name = com.column_name and col.table_name = com.table_name and col.owner = com.owner "
				+") ";
		if(schemaPattern!=null) {
			sql += "where TABLE_SCHEM = ? ";
			params.add(schemaPattern);
		}
		if(tableNamePattern!=null) {
			if(schemaPattern!=null) {
				sql += "and ";
			}
			else {
				sql += "where ";
			}
			sql += " TABLE_NAME = ? ";
			params.add(tableNamePattern);
		}
		sql += "order by TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION ";
		
		PreparedStatement st = conn.prepareStatement(sql);
		for(int i=0;i<params.size();i++) {
			st.setString(i+1, params.get(i));
		}
		log.debug("sql:\n"+sql);
		return st.executeQuery();
	}
	
	static boolean grabFKFromUK = false;
	
	/**
	 * added a UK_CONSTRAINT_TYPE column, which returns: P - primary key, U - unique key
	 */
	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		return getKeys(catalog, schema, table, true);
	}
	
	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		return getKeys(catalog, schema, table, false);
	}
	
	public ResultSet getKeys(String catalog, String schema, String table, boolean imported) throws SQLException {
		//XXX: if(grabFKFromUK==true) -> status, validated, rely are not returned! 
		if(!grabFKFromUK) {
			if(imported) {
				return super.getImportedKeys(catalog, schema, table);
			}
			else {
				return super.getExportedKeys(catalog, schema, table);
			}
		}
		
		Connection conn = metadata.getConnection();
		List<String> params = new ArrayList<String>();

		String sql = "select * from (";
		sql += "select '' as PKTABLE_CAT, acuk.owner as PKTABLE_SCHEM, acuk.table_name as PKTABLE_NAME, accuk.column_name as PKCOLUMN_NAME, \n"
				+"       '' as FKTABLE_CAT, acfk.owner as FKTABLE_SCHEM, acfk.table_name as FKTABLE_NAME, accfk.column_name as FKCOLUMN_NAME, \n"
				+"       accuk.position as KEY_SEQ, -1 as UPDATE_RULE, "
				+"       decode(acfk.DELETE_RULE, "
				+"'CASCADE', "+DatabaseMetaData.importedKeyCascade+", "
				+"'NO ACTION', "+DatabaseMetaData.importedKeyNoAction+", "
				+"'SET DEFAULT', "+DatabaseMetaData.importedKeySetDefault+", "
				+"'SET NULL', "+DatabaseMetaData.importedKeySetNull+", "
				+"-1) as DELETE_RULE, \n"
				+"       acfk.constraint_name as FK_NAME, acfk.r_constraint_name as PK_NAME, '' as DEFERRABILITY, \n"
				+"       acuk.constraint_type as UK_CONSTRAINT_TYPE, " //returns type of unique key: P - primary, U - unique
				+"       acfk.status, acfk.validated, acfk.rely "
				+"from "
				+(useDbaMetadataObjects?"dba_constraints acfk, dba_cons_columns accfk, dba_constraints acuk, dba_cons_columns accuk \n":
					"all_constraints acfk, all_cons_columns accfk, all_constraints acuk, all_cons_columns accuk \n")
				+"where acfk.owner = accfk.owner and acfk.constraint_name = accfk.constraint_name and acfk.constraint_type = 'R' \n"
				+"  and acuk.owner = accuk.owner and acuk.constraint_name = accuk.constraint_name and acuk.constraint_type in ('P','U') \n"
				+"  and acfk.r_owner = acuk.owner and acfk.r_constraint_name = acuk.constraint_name \n"
				+"  and accfk.position = accuk.position \n" 
				+"order by acfk.owner, acfk.constraint_name, accfk.position "
				+") ";

		if(schema!=null) {
			sql += "where "+(imported?"FKTABLE_SCHEM":"PKTABLE_SCHEM")+" = ? \n";
			params.add(schema);
		}
		if(table!=null) {
			if(schema!=null) {
				sql += "and ";
			}
			else {
				sql += "where ";
			}
			sql += (imported?"FKTABLE_NAME":"PKTABLE_NAME")+" = ? ";
			params.add(table);
		}
		sql += "order by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ ";
		PreparedStatement st = conn.prepareStatement(sql);
		for(int i=0;i<params.size();i++) {
			st.setString(i+1, params.get(i));
		}
		log.debug("sql:\n"+sql);
		return st.executeQuery();
	}
	
	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		Connection conn = metadata.getConnection();
		
		String sql = "select null as table_cat, " +
				"  acc.owner as table_schem, " +
				"  acc.table_name, " +
				"  acc.column_name, " +
				"  acc.position as key_seq, " +
				"  acc.constraint_name as pk_name " +
				"\nfrom " +
				(useDbaMetadataObjects?"dba_constraints ac, dba_cons_columns acc ":"all_constraints ac, all_cons_columns acc ") +
				"\nwhere ac.constraint_name = acc.constraint_name " +
				"  and ac.table_name = acc.table_name " +
				"  and ac.owner = acc.owner " +
				"  and ac.constraint_type = 'P' " +
				"  and ac.owner = ? " +
				"  and ac.table_name = ? " +
				"\norder by acc.position ";
		
		log.debug("sql:\n"+sql);
		PreparedStatement st = conn.prepareStatement(sql);
		st.setString(1, schema);
		st.setString(2, table);
		return st.executeQuery();
	}
}
