package tbrugz.sqldump.dbmsfeatures;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.ExecutableParameter;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.TableType;

/*
 * TODO: add grab materialized views...
 */
public class PostgreSQLFeatures extends PostgreSQLAbstractFeatures {

	static Log log = LogFactory.getLog(PostgreSQLFeatures.class);
	
	@Override
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) throws SQLException {
		return new PostgreSqlDatabaseMetaData(metadata);
	}
	
	@Override
	String grabDBRoutinesQuery(String schemaPattern, String execNamePattern) {
		return "select routine_name, routine_type, data_type, external_language, routine_definition "
				+" , (select array_agg(parameter_name::text order by ordinal_position) from information_schema.parameters p where p.specific_name = r.specific_name) as parameter_names "
				+" , (select array_agg(data_type::text order by ordinal_position) from information_schema.parameters p where p.specific_name = r.specific_name) as parameter_types "
				+"from information_schema.routines r "
				+"where routine_definition is not null "
				+"and specific_schema = '"+schemaPattern+"' "
				+(execNamePattern!=null?"and routine_name = '"+execNamePattern+"' ":"")
				+"order by routine_catalog, routine_schema, routine_name ";
	}
	
	@Override
	public void grabDBExecutables(Collection<ExecutableObject> execs, String schemaPattern, String execNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing executables");
		String query = grabDBRoutinesQuery(schemaPattern, execNamePattern);
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		int rowcount = 0;
		while(rs.next()) {
			InformationSchemaRoutine eo = new InformationSchemaRoutine();
			eo.setSchemaName( schemaPattern );
			eo.setName( rs.getString(1) );
			try {
				eo.setType( DBObjectType.parse(rs.getString(2)) );
			}
			catch(IllegalArgumentException iae) {
				log.warn("unknown object type: "+rs.getString(2));
				eo.setType( DBObjectType.EXECUTABLE );
			}
			ExecutableParameter ep = new ExecutableParameter();
			ep.setDataType(rs.getString(3));
			//eo.returnType = rs.getString(3);
			eo.setReturnParam(ep);
			
			eo.externalLanguage = rs.getString(4);
			eo.setBody( rs.getString(5) );
			
			//parameters!
			Array paramsArr = rs.getArray(6);
			if(paramsArr!=null) {
				String[] params = (String[]) paramsArr.getArray();
				//eo.parameterNames = Arrays.asList(params);
				String[] paramTypes = (String[]) rs.getArray(7).getArray();
				//eo.parameterTypes = Arrays.asList(paramTypes);
				List<ExecutableParameter> lpar = new ArrayList<ExecutableParameter>();
				for(int i=0;i<params.length;i++) {
					ExecutableParameter epar = new ExecutableParameter();
					epar.setName(params[i]);
					epar.setDataType(paramTypes[i]);
					lpar.add(epar);
				}
				if(lpar.size()>0) {
					eo.setParams(lpar);
				}
			}
			
			if(addExecutableToModel(execs, eo)) {
				count++;
			}
			
			rowcount++;
		}
		
		rs.close();
		st.close();
		log.info("["+schemaPattern+"]: "+count+" executable objects/routines grabbed [rowcount="+rowcount+"; all-executables="+execs.size()+"]");
	}
	
	@Override
	public boolean supportsExplainPlan() {
		return true;
	}
	
	/*
	 * http://www.postgresql.org/docs/devel/static/sql-explain.html
	 */
	@Override
	public ResultSet explainPlan(String sql, List<Object> params, Connection conn) throws SQLException {
		String expsql = sqlExplainPlanQuery(sql);
		return bindAndExecuteQuery(expsql, params, conn);
	}
	
	@Override
	public String sqlExplainPlanQuery(String sql) {
		return "explain verbose "+sql;
	}
	
	/*
	 * TODOne: add support for foreign tables..
	 * 
	 * create FOREIGN table (...)
	 * 
	 * footer:
	 * SERVER servername
	 * OPTIONS (schema 'SCHEMA', table 'TABLE');
	 */
	
	/*
	@Override
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) {
		return new PostgreSqlDatabaseMetaData(metadata);
	}*/
	
	@Override
	public Table getTableObject() {
		return new PostgreSqlTable();
	}

	/*
	@Override
	public void addTableSpecificFeatures(Table t, ResultSet rs) {
	}
	*/
	
	// postgresql 9.1+
	@Override
	public void addTableSpecificFeatures(Table t, Connection conn) throws SQLException {
		if(t.getType().equals(TableType.FOREIGN_TABLE)) {
			PostgreSqlTable pt = (PostgreSqlTable) t;
			
			String server = getForeignTableServer(conn, pt.getSchemaName(), pt.getName());
			Map<String,String> options = getForeignTableOptions(conn, pt.getSchemaName(), pt.getName());
			pt.setForeignTableServer(server);
			pt.setForeignTableOptions(options);
		}
	}
	
	String getForeignTableServer(Connection conn, String schema, String name) throws SQLException {
		String ret = null;
		
		String sql = "select foreign_table_schema, foreign_table_name, foreign_server_name\n" +
				"from information_schema.foreign_tables\n" +
				"where foreign_table_schema = ?\n" +
				"and foreign_table_name = ?";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setString(1, schema);
		ps.setString(2, name);
		ResultSet rs = ps.executeQuery();
		if(rs.next()) {
			ret = rs.getString(3);
		}
		
		return ret;
	}

	Map<String,String> getForeignTableOptions(Connection conn, String schema, String name) throws SQLException {
		Map<String,String> ret = new LinkedHashMap<String, String>();
		
		String sql = "select foreign_table_schema, foreign_table_name, option_name, option_value\n" + 
				"from information_schema.foreign_table_options\n" + 
				"where foreign_table_schema = ?\n" + 
				"and foreign_table_name = ?";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setString(1, schema);
		ps.setString(2, name);
		ResultSet rs = ps.executeQuery();
		while(rs.next()) {
			String key = rs.getString(3);
			String value = rs.getString(4);
			ret.put(key, value);
		}
	
		return ret;
	}
	
}
