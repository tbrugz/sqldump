package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.AbstractDatabaseMetaDataDecorator;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.util.Utils;

/**
 * Does not create decorator for DatabaseMetaData - use driver plain methods
 * 
 * known limitations:
 * - oracle's driver getTables() does not seems to distinguish TABLEs from MATERIALIZED VIEWs: using SimpleDBMD
 *   addresses this problem - maybe...
 */
public class OracleFeaturesLite extends OracleFeatures {
	private static Log log = LogFactory.getLog(OracleFeaturesLite.class);
	
	public static final String PROP_USE_SIMPLE_DBMD = "sqldump.dbms.oraclelite.use-simple-dbmd";
	
	/**
	 * Adds a getTables() method that knows of materialized views
	 */
	public class SimpleDBMD extends AbstractDatabaseMetaDataDecorator {
		
		static final String T = "TABLE";
		static final String S = "SYNONYM";
		static final String V = "VIEW";
		static final String MV = "MATERIALIZED VIEW";
		
		public SimpleDBMD(DatabaseMetaData metadata) {
			super(metadata);
		}
		
		@Override
		public ResultSet getTables(String catalog, String schemaPattern,
				String tableNamePattern, String[] types) throws SQLException {
			List<String> ltypes = null;
			if(types!=null) { ltypes = Arrays.asList(types); };
			
			StringBuilder sb = new StringBuilder();
			List<String> params = new ArrayList<String>();
			
			boolean addMV = ltypes==null || ltypes.contains(MV);
			sb.append("select distinct table_cat, table_schem, table_name, table_type, remarks "
				+"\nfrom (select null as table_cat, o.owner as table_schem, o.object_name as table_name, ");
			if(addMV) {
				sb.append("case when mv.owner is not null then 'MATERIALIZED VIEW' else o.object_type end as table_type, ");
			}
			else {
				sb.append("o.object_type as table_type, ");
			}
			sb.append("null as remarks " +
				"from all_objects o ");
			if(addMV) {
				sb.append("left outer join all_mviews mv on o.owner = mv.owner and o.object_name = mv.mview_name ");
			}
			//sb.append("\nwhere object_type != 'TABLE' or (o.owner, o.object_name) not in (select owner, mview_name from all_mviews) ");
			sb.append(") s");
			
			/*sb.append("select * from (select null as table_cat, o.owner as table_schem, o.object_name as table_name, "+
					"o.object_type as table_type, null as remarks "+
					"\nfrom all_objects o"+
					"\nwhere object_type != 'TABLE' or (owner, object_name) not in (select owner, mview_name from all_mviews)) s");*/
			
			// object_type
			sb.append("\nwhere table_type in (");
			boolean added1st = false;
			if(ltypes==null || ltypes.contains(T)) {
				sb.append("'").append(T).append("'"); added1st = true;
			}
			if(ltypes==null || ltypes.contains(S)) {
				if(added1st) { sb.append(", "); };
				sb.append("'").append(S).append("'"); added1st = true;
			}
			if(ltypes==null || ltypes.contains(V)) {
				if(added1st) { sb.append(", "); };
				sb.append("'").append(V).append("'"); added1st = true;
			}
			if(ltypes==null || ltypes.contains(MV)) {
				if(added1st) { sb.append(", "); };
				sb.append("'").append(MV).append("'"); added1st = true;
			}
			if(!added1st) {
				sb.append("null");
			}
			sb.append(") ");
			
			if(schemaPattern!=null) {
				sb.append("and table_schem like ? ");
				params.add(schemaPattern);
			}
			if(tableNamePattern!=null) {
				sb.append("and table_name like ? ");
				params.add(tableNamePattern);
			}
			sb.append("order by table_schem, table_name");
			
			log.debug("sql:\n"+sb);
			
			/*if(!added1st) {
				throw new SQLException("not one table type valid: "+ltypes);
			}*/
			
			Connection conn = metadata.getConnection();
			PreparedStatement st = conn.prepareStatement(sb.toString());
			for(int i=0;i<params.size();i++) {
				st.setString(i+1, params.get(i));
			}
			return st.executeQuery();
		}
	}
	
	boolean useSimpleDBMD = false;
	
	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		useSimpleDBMD = Utils.getPropBool(prop, PROP_USE_SIMPLE_DBMD, useSimpleDBMD);
	}
	
	@Override
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) {
		if(useSimpleDBMD) {
			return new SimpleDBMD(metadata);
		}
		return metadata;
	}
	
	/*
	 * not adding column specific features since this class does not use OracleDatabaseMetaData
	 */
	@Override
	public void addColumnSpecificFeatures(Column c, ResultSet rs) {
	}
	
	/*
	 * not adding table specific features since this class does not use OracleDatabaseMetaData
	 */
	@Override
	public void addTableSpecificFeatures(Table t, ResultSet rs) {
	}
	
	/*
	 * not adding FK specific features since this class does not use OracleDatabaseMetaData
	 */
	@Override
	public void addFKSpecificFeatures(FK fk, ResultSet rs) {
	}

}
