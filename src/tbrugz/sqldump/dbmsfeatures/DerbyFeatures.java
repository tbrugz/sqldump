package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.DefaultDBMSFeatures;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;
import tbrugz.sqldump.util.StringUtils;

/*
 * see: https://db.apache.org/derby/docs/10.14/tools/ctoolsdblook.html
 */
public class DerbyFeatures extends DefaultDBMSFeatures {

	static final Log log = LogFactory.getLog(DerbyFeatures.class);

	/*@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		log.info("procProperties: "+prop);
	}*/

	@Override
	public void grabDBObjects(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		if(grabViews) {
			grabDBViews(model.getViews(), schemaPattern, null, conn);
		}
		if(grabTriggers) {
			grabDBTriggers(model.getTriggers(), schemaPattern, null, null, conn);
		}
		
		try {
			if(grabSequences) { 
				grabDBSequences(model.getSequences(), schemaPattern, null, conn);
			}
		}
		catch(SQLSyntaxErrorException e) {
			String currentVersion = ""+conn.getMetaData().getDatabaseMajorVersion()+"."+
					conn.getMetaData().getDatabaseMinorVersion();
			log.warn("can't grab derby sequences. database version 10.6+ required [currentVersion="+currentVersion+"]");
			log.debug("nested exception: "+e);
		}

		if(grabUniqueConstraints) {
			grabDBUniqueConstraints(model.getTables(), schemaPattern, null, null, conn);
		}

		if(grabExecutables) {
			grabDBExecutables(model.getExecutables(), schemaPattern, null, conn);
		}
		
		//XXX: derby: add synonyms?
		//XXX: derby: add check constraints?
	}

	@Override
	public void grabDBViews(Collection<View> views, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing views");
		//String query = "select tableid, viewdefinition "
		//		+"from sys.sysviews "
		//		+"order by tableid ";
		String query = "select sc.schemaname, st.tablename, sv.tableid, sv.viewdefinition "
				+"from sys.systables as st "
				+"join sys.sysviews as sv on st.tableid = sv.tableid "
				+"join sys.sysschemas as sc on st.schemaid = sc.schemaid "
				+"where 1=1 "
				+(schemaPattern!=null?"and sc.schemaname = '"+schemaPattern+"' ":"")
				+(viewNamePattern!=null?"and st.tablename = '"+viewNamePattern+"'":"")
				+"order by st.tablename ";
		log.debug("grabbing views query: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			View v = new View();
			v.setSchemaName( rs.getString(1) );
			v.setName( rs.getString(2) );
			//v.query = getStringFromReader(rs.getCharacterStream(3));
			v.setQuery( rs.getString(4) );
			views.add(v);
			count++;
		}
		
		rs.close();
		st.close();
		log.info(count+" views grabbed");
	}

	@Override
	public void grabDBTriggers(Collection<Trigger> triggers, String schemaPattern, String tableNamePattern, String triggerNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing triggers");
		String query = "select triggername, event, firingtime, type, st.tableid, sc.schemaname, tablename, triggerdefinition "
				+"from sys.systables as st "
				+"join sys.systriggers as tr on st.tableid = tr.tableid "
				+"join sys.sysschemas as sc on st.schemaid = sc.schemaid "
				+"where 1=1"
				+(schemaPattern!=null?" and sc.schemaname = '"+schemaPattern+"' ":"")
				+(tableNamePattern!=null?" and st.tablename = '"+tableNamePattern+"'":"")
				+(triggerNamePattern!=null?" and triggername ='"+triggerNamePattern+"'":"");
		log.debug("grabbing triggers query: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		//InformationSchemaTrigger.addSplitter = true;
		
		int count = 0;
		while(rs.next()) {
			InformationSchemaTrigger t = new InformationSchemaTrigger();
			//t.addSplitter = true;
			t.setName( rs.getString(1) );
			//t.tableName = rs.getString(3);
			//t.description = rs.getString(4);
			//t.description = t.schemaName+"."+t.name+" as";
			//t.body = rs.getString(2);
			
			//XXX: derby REFERENCING?
			
			//t.schemaName = rs.getString(2);
			//t.name = rs.getString(1);
			String event = rs.getString(2);
			t.eventsManipulation.add("I".equals(event)?"INSERT":"U".equals(event)?"UPDATE":"DELETE");
			t.setSchemaName(rs.getString(6));
			t.setTableName(rs.getString(7));
			t.actionStatement = rs.getString(8);
			String forEachX = rs.getString(4);
			t.actionOrientation = "R".equals(forEachX)?"ROW":"STATEMENT";
			String timing = rs.getString(3);
			t.conditionTiming = "A".equals(timing)?"AFTER":"BEFORE";
			
			triggers.add(t);
			count++;
		}
		
		rs.close();
		st.close();
		log.info(count+" triggers grabbed");
	}
	
	@Override
	public void grabDBSequences(Collection<Sequence> seqs, String schemaPattern, String sequenceNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing sequences");
		String query = "select sequencename, minimumvalue, maximumvalue, currentvalue, increment, sequencedatatype, sc.schemaname "
				+"from sys.syssequences ss "
				+"join sys.sysschemas as sc on ss.schemaid = sc.schemaid "
				+"where 1=1"
				+(schemaPattern!=null?" and sc.schemaname = '"+schemaPattern+"' ":"")
				+(sequenceNamePattern!=null?" and sequencename = '"+sequenceNamePattern+"' ":"")
				+"order by sequencename ";
		log.debug("grabbing sequences query: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Sequence s = new Sequence();
			s.setName( rs.getString(1) );
			s.setMinValue(rs.getLong(2));
			s.setMaxValue(rs.getLong(3));
			s.setLastNumber(rs.getLong(4));
			s.setIncrementBy(rs.getLong(5));
			s.setSchemaName( rs.getString(7) );
			seqs.add(s);
			count++;
		}
		
		rs.close();
		st.close();
		log.info(count+" sequences grabbed");
	}
	
	/**
	 * see: [SYS.SYSALIASES system table](https://db.apache.org/derby/docs/10.14/ref/rrefsistabs28114.html)
	 */
	@Override
	public void grabDBExecutables(Collection<ExecutableObject> execs, String schemaPattern, String execNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing executables");
		String query = "select s.schemaname, a.alias, a.aliastype, a.javaclassname, a.aliasinfo\n" + 
				"from sys.sysaliases a\n" + 
				"inner join sys.sysschemas s on a.schemaid = s.schemaid\n" + 
				"where not a.systemalias\n" +
				"  and a.aliastype in ('F', 'P', 'G')" +
				(schemaPattern!=null?"\nand s.schemaname like '"+schemaPattern+"'":"");
		log.debug("grabbing executables query: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int countRows = 0, countAdded = 0;
		while(rs.next()) {
			ExecutableObject eo = new ExecutableObject();
			eo.setSchemaName(rs.getString(1));
			eo.setName(rs.getString(2));
			
			String aliasType = rs.getString(3);
			eo.setType(getExecutableType(aliasType));
			
			String javaClass = rs.getString(4);
			
			String aliasInfo = null;
			try {
				aliasInfo = rs.getString(5);
			}
			catch(SQLException e) {
				log.warn("Error grabbing aliasinfo of executable (full derby.jar may be needed in classpath): "+e);
				log.debug("Error grabbing aliasinfo of executable: "+e.getMessage(), e);
				break;
			}
			
			//log.info("aliasType '"+aliasType+"; 'aliasInfo: "+aliasInfo);
			String body = null;
			
			if(!eo.getType().equals(DBObjectType.AGGREGATE)) {
				// functions & procedures
				int idx1 = aliasInfo.indexOf("(");
				int idx2 = aliasInfo.lastIndexOf("RETURNS");
				if(idx2==-1) {
					idx2 = aliasInfo.lastIndexOf(")");
				}
				else {
					idx2 = aliasInfo.lastIndexOf(")", idx2);
				}
				String methodName = aliasInfo.substring(0, idx1).trim();
				String parameters = aliasInfo.substring(idx1+1, idx2).trim();
				String xtraInfo = aliasInfo.substring(idx2+1).trim();
				body = eo.getType()+" "+eo.getName()+" ("+parameters+") "+
						xtraInfo+
						"\n\texternal name '"+javaClass+"."+methodName+"'";
			}
			else {
				// derby aggregates
				body = "derby aggregate "+eo.getName()+" "+
						aliasInfo+
						"\n\texternal name '"+javaClass+"'";
			}
			eo.setBody(body+";");
			boolean added = execs.add(eo);
			if(added) { countAdded++; }
			countRows++;
		}
		
		rs.close();
		st.close();
		log.info(countAdded+" executables grabbed");
		if(countRows!=countAdded) {
			log.warn(countRows+" executables found but "+countAdded+" grabbed (were already added to model)");
		}
	}
	
	DBObjectType getExecutableType(String aliasType) {
		if(aliasType.equals("F")) {
			return DBObjectType.FUNCTION;
		}
		if(aliasType.equals("P")) {
			return DBObjectType.PROCEDURE;
		}
		if(aliasType.equals("G")) {
			return DBObjectType.AGGREGATE;
		}
		log.warn("unknown executable type: "+aliasType);
		return DBObjectType.EXECUTABLE;
	}
	
	String grabDBUniqueConstraintsQuery(String schemaPattern, String tableNamePattern, String constraintNamePattern) {
		return "select sc.schemaname, t.tablename, c.constraintname, cg.descriptor "
				+ "from sys.sysconstraints c "
				+ "join sys.syskeys k on c.constraintid = k.constraintid "
				+ "join sys.sysconglomerates cg on k.conglomerateid = cg.conglomerateid "
				+ "join sys.sysschemas sc on c.schemaid = sc.schemaid "
				+ "join sys.systables t on c.tableid = t.tableid "
				+ "where type = 'U' "
				+(schemaPattern!=null?"and sc.schemaname = '"+schemaPattern+"' ":"")
				+(tableNamePattern!=null?"and t.tablename = '"+tableNamePattern+"' ":"")
				+(constraintNamePattern!=null?"and c.constraintname = '"+constraintNamePattern+"' ":"");
	}

	static final Pattern indexesPattern = Pattern.compile(".*\\(([0-9 ,]+)\\).*");

	/*
	 * see: https://stackoverflow.com/questions/2349785/how-to-get-primary-key-and-unique-constraint-columns-in-derby
	 */
	@Override
	public void grabDBUniqueConstraints(Collection<Table> tables, String schemaPattern, String tableNamePattern, String constraintNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing unique constraints");
		String query = grabDBUniqueConstraintsQuery(schemaPattern, tableNamePattern, constraintNamePattern);
		Statement st = conn.createStatement();
		log.debug("grabbing unique constraints query: "+query);
		ResultSet rs = st.executeQuery(query);
		
		int countUKs = 0;
		int countCols = 0;
		while(rs.next()) {
			String schemaName = rs.getString(1);
			String tableName = rs.getString(2);
			
			Constraint c = new Constraint();
			c.setType(Constraint.ConstraintType.UNIQUE);
			c.setName( rs.getString(3) );
			String descriptor = null;
			try {
				descriptor = rs.getString(4);
			}
			catch(SQLException e) {
				log.warn("Error grabbing descriptor of unique constraint (full derby.jar may be needed in classpath): "+e);
				log.debug("Error grabbing descriptor of unique constraint: "+e.getMessage(), e);
				break;
			}
			if(descriptor == null) {
				log.warn("grabDBUniqueConstraints: [constraint "+c+"] null descriptor");
				continue;
			}
			Matcher matcher = indexesPattern.matcher(descriptor);
			if(!matcher.matches()) {
				log.warn("grabDBUniqueConstraints: [constraint "+c+"] unrecognizable descriptor: "+descriptor);
				continue;
			}
			String columnList = matcher.group(1);
			String[] colIdxs = columnList.split(",");
			Table t = DBIdentifiable.getDBIdentifiableBySchemaAndName(tables, schemaName, tableName);
			if(t!=null) {
				List<String> tCols = t.getColumnNames();
				for(String ss: colIdxs) {
					int i = Integer.parseInt(ss.trim());
					String colName = tCols.get(i-1);
					c.getUniqueColumns().add(colName);
					log.trace("grabDBUniqueConstraints: added column #"+i+": "+colName);
				}
				countCols += colIdxs.length;
				countUKs++;
				t.getConstraints().add(c);
			}
			else {
				log.warn("constraint "+c+" can't be added to table '"+tableName+"': table not found");
			}
		}
		
		rs.close();
		st.close();
		log.info(countUKs+" unique constraints grabbed [colcount="+countCols+"]");
	}

	@Override
	public boolean supportsExplainPlan() {
		return true;
	}
	
	/*
	 * Explain plan
	 * 
	 * see:
	 * http://stackoverflow.com/questions/5981406/apache-derby-explain-select
	 * https://db.apache.org/derby/docs/10.14/tuning/ctun_xplain_style.html
	 * https://db.apache.org/derby/docs/10.14/tuning/ctun_xplain_tables.html
	 * https://db.apache.org/derby/docs/10.14/ref/rrefsysxplain_statements.html
	 */
	@Override
	public ResultSet explainPlan(String sql, List<Object> params,
			Connection conn) throws SQLException {
		
		String planSchema = "XPLAIN_SCHEMA";
		//String cursorName = "xyz";

		log.debug("explainPlan: schema="+planSchema); //+" ; cursorName="+cursorName);
		
		// turn on RUNTIMESTATISTICS for connection
		conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)").execute();
		conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)").execute();

		// https://db.apache.org/derby/docs/10.9/ref/rref_syscs_set_xplain_mode.html
		// 1: statements are compiled and optimized, but not executed
		// 0: (default) statements are compiled, optimized, and executed normally
		conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_XPLAIN_MODE(1)").execute();
		
		// Indicate that statistics information should be captured into
		conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_XPLAIN_SCHEMA('"+planSchema+"')").execute();

		//ResultSet rs =
		bindAndExecuteQuery(sqlExplainPlanQuery(sql), params, conn); //, cursorName
		
		conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)").execute();
		//conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)").execute();
		//conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_XPLAIN_MODE(0)").execute();
		//conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_XPLAIN_SCHEMA('')").execute();
		
		String planQuery =
			"select " +
			//"     st.stmt_name, st.stmt_text, \n" +
			"     rs.op_identifier, rs.op_details, rs.lock_mode, rs.lock_granularity, rs.est_row_count, rs.est_cost, \n" + 
			"     sp.scan_object_name, sp.scan_object_type, sp.scan_type \n" + 
			"from "+planSchema+".sysxplain_statements st \n" + 
			"join "+planSchema+".sysxplain_resultsets rs on st.stmt_id = rs.stmt_id \n" + 
			"left outer join "+planSchema+".sysxplain_scan_props sp on rs.scan_rs_id = sp.scan_rs_id \n" + 
			"where 1=1\n" + 
			//"and trim(both '\n' from trim(st.stmt_text)) = trim(both '\n' from trim(?))";
			"and st.stmt_text = ?";
			//"and st.stmt_name = ?";
			//XXX trim: remove all heading & trailing whitespaces without regex?
		
		List<Object> planQueryParams = Arrays.asList((Object) StringUtils.lrtrim(sql));
		//List<Object> planQueryParams = Arrays.asList((Object)cursorName);
		//List<Object> planQueryParams = null;

		log.debug("explainPlan: sql [schema="+planSchema+"]: "+planQuery); //+";cursorName="+cursorName

		return bindAndExecuteQuery(planQuery, planQueryParams, conn);
	}
	
	@Override
	public String sqlExplainPlanQuery(String sql) {
		return sql;
	}
	
	// see: https://db.apache.org/derby/docs/10.13/ref/rrefsqlj16762.html
	@Override
	public String sqlLengthFunctionByType(String columnName, String columnType) {
		return "length("+columnName+")";
	}
	
	@Override
	public String sqlIsNullFunction(String columnName) {
		return columnName+" is null";
	}
	
}
