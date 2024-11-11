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
import tbrugz.sqldump.dbmodel.BaseNamedDBObject;
import tbrugz.sqldump.dbmodel.PartitionType;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.TableType;

/*
 * see:
 * https://www.postgresql.org/docs/current/information-schema.html
 *
 * TODO: add grab materialized views...
 */
public class PostgreSQLFeatures extends PostgreSQLAbstractFeatures {

	static final Log log = LogFactory.getLog(PostgreSQLFeatures.class);
	
	@Override
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) throws SQLException {
		return new PostgreSqlDatabaseMetaData(metadata);
	}
	
	@Override
	String grabDBRoutinesQuery(String schemaPattern, String execNamePattern) {
		return "select routine_name, routine_type, data_type, external_language, routine_definition, external_name, is_deterministic "
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
			eo.setReturnParam(ep);
			//log.debug("exec="+eo+" [exec="+eo.getName()+"]");
			
			eo.externalLanguage = rs.getString(4);
			eo.setBody( rs.getString(5) );
			
			//rs.getString(6) - external_name
			//rs.getString(7) - is_deterministic
			
			//parameters!
			Array paramsArr = rs.getArray(8);
			if(paramsArr!=null) {
				String[] params = (String[]) paramsArr.getArray();
				//eo.parameterNames = Arrays.asList(params);
				String[] paramTypes = (String[]) rs.getArray(9).getArray();
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
				//log.debug("lpar="+lpar+" [exec="+eo.getName()+"]");
			}
			else {
				//log.debug("paramsArr is null [exec="+eo.getName()+"]");
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
		if(!(t instanceof PostgreSqlTable)) { return; }

		PostgreSqlTable pt = (PostgreSqlTable) t;
		if(pt.getType().equals(TableType.FOREIGN_TABLE)) {
			String server = getForeignTableServer(conn, pt.getSchemaName(), pt.getName());
			Map<String,String> options = getForeignTableOptions(conn, pt.getSchemaName(), pt.getName());
			pt.setForeignTableServer(server);
			pt.setForeignTableOptions(options);
		}
		if(pt.getType().equals(TableType.PARTITIONED_TABLE)) {
			addPartitionInfo(pt, getPartitionTableInfo(conn, pt.getSchemaName(), pt.getName()));
		}
		if(pt.getType().equals(TableType.TABLE_PARTITION)) {
			addTablePartitionInfo(pt, getTablePartitionInfo(conn, pt.getSchemaName(), pt.getName()));
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
	
	// see: https://itectec.com/database/postgresql-how-to-identify-the-column-used-to-partition-a-table-from-the-postgres-system-catalogs/
	// XXX order by: num_columns? column_index?
	ResultSet getPartitionTableInfo(Connection conn, String schema, String name) throws SQLException {
		String sql = "select \n"
				+ "    par.relnamespace::regnamespace::text as table_schema, \n"
				+ "    par.relname as table_name,\n"
				+ "    pt.partition_strategy,\n"
				+ "    partnatts as num_columns,\n"
				+ "    column_index,\n"
				+ "    col.column_name\n"
				+ "from (\n"
				+ "    select partrelid, partnatts,\n"
				+ "         case partstrat\n"
				+ "              when 'h' then 'hash' \n"
				+ "              when 'l' then 'list' \n"
				+ "              when 'r' then 'range' end as partition_strategy,\n"
				+ "         unnest(partattrs) column_index\n"
				+ "	from pg_partitioned_table\n"
				+ "	) pt \n"
				+ "join pg_class par on par.oid = pt.partrelid\n"
				+ "join information_schema.columns col on col.table_schema = par.relnamespace::regnamespace::text\n"
				+ "	and col.table_name = par.relname and ordinal_position = pt.column_index\n"
				+ "where 1=1\n"
				+ "	and par.relnamespace::regnamespace::text = ?"
				+ "	and par.relname = ?";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setString(1, schema);
		ps.setString(2, name);
		return ps.executeQuery();
	}
	
	void addPartitionInfo(PostgreSqlTable pgtable, ResultSet rs) throws SQLException {
		pgtable.partitionColumns = new ArrayList<>();
		for(int i=0;rs.next();i++) {
			if(i==0) {
				String partitionStrategy = rs.getString(3);
				pgtable.partitionType = PartitionType.valueOf(partitionStrategy.toUpperCase());
			}
			String columnName = rs.getString(6);
			pgtable.partitionColumns.add(columnName);
		}
		return;
	}

	/*
	 * see: https://dba.stackexchange.com/a/221283
	 */
	ResultSet getTablePartitionInfo(Connection conn, String schema, String name) throws SQLException {
		String sql = "select base_tb.relnamespace::regnamespace::text as table_schema,\n"
				+ "  base_tb.relname as table_name,\n"
				+ "  pt.relname as partition_name,\n"
				+ "  pg_get_expr(pt.relpartbound, pt.oid, true) as partition_expression\n"
				+ "from pg_class base_tb \n"
				+ "join pg_inherits i on i.inhparent = base_tb.oid\n"
				+ "join pg_class pt on pt.oid = i.inhrelid\n"
				+ "where 1=1\n"
				+ "  and pt.relnamespace::regnamespace::text = ?"
				+ "  and pt.relname = ?";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setString(1, schema);
		ps.setString(2, name);
		return ps.executeQuery();
	}
	
	void addTablePartitionInfo(PostgreSqlTable pgtable, ResultSet rs) throws SQLException {
		for(int i=0;rs.next();i++) {
			if(i>0) {
				log.warn("addPartitionedTableInfo: more than 1 row? i=="+i);
			}
			pgtable.baseTable = new BaseNamedDBObject(rs.getString(1), rs.getString(2));
			pgtable.partitionExpression = rs.getString(4);
		}
		return;
	}

}
