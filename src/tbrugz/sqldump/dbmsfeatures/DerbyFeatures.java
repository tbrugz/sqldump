package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.DefaultDBMSFeatures;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;

/*
 * see: https://db.apache.org/derby/docs/10.14/tools/ctoolsdblook.html
 */
public class DerbyFeatures extends DefaultDBMSFeatures {
	static Log log = LogFactory.getLog(DerbyFeatures.class);

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
			log.warn("can't grab derby sequences. database version 10.6+ required"); //XXX output current derby db version?
			log.debug("nested exception: "+e);
		}
		
		if(grabExecutables) {
			grabDBExecutables(model.getExecutables(), schemaPattern, null, conn);
		}
		
		//XXX: derby: add synonyms?
		//XXX: derby: add check/unique constraints?
	}

	@Override
	public void grabDBViews(Collection<View> views, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing views");
		//String query = "select tableid, viewdefinition "
		//		+"from sys.sysviews "
		//		+"order by tableid ";
		String query = "select st.tablename, sv.tableid, sv.viewdefinition "
				+"from sys.systables as st, sys.sysviews as sv "
				+"where st.tableid = sv.tableid "
				+(viewNamePattern!=null?"and st.tablename = '"+viewNamePattern+"'":"")
				+"order by tablename ";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			View v = new View();
			v.setName( rs.getString(1) );
			//v.query = getStringFromReader(rs.getCharacterStream(3));
			v.setQuery( rs.getString(3) );
			v.setSchemaName( schemaPattern );
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
		String query = "select triggername, event, firingtime, type, st.tableid, tablename, triggerdefinition "
				+"from sys.systables as st, sys.systriggers as tr "
				+"where st.tableid = tr.tableid "
				+(triggerNamePattern!=null?"and triggername ='"+triggerNamePattern+"'":"");
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		//InformationSchemaTrigger.addSplitter = true;
		
		int count = 0;
		while(rs.next()) {
			InformationSchemaTrigger t = new InformationSchemaTrigger();
			//t.addSplitter = true;
			t.setName( rs.getString(1) );
			t.setSchemaName( schemaPattern );
			//t.tableName = rs.getString(3);
			//t.description = rs.getString(4);
			//t.description = t.schemaName+"."+t.name+" as";
			//t.body = rs.getString(2);
			
			//XXX: derby REFERENCING?
			
			//t.schemaName = rs.getString(2);
			//t.name = rs.getString(1);
			String event = rs.getString(2);
			t.eventsManipulation.add("I".equals(event)?"INSERT":"U".equals(event)?"UPDATE":"DELETE");
			t.setTableName(rs.getString(6));
			t.actionStatement = rs.getString(7);
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
		String query = "select sequencename, minimumvalue, maximumvalue, currentvalue, increment, sequencedatatype "
				+"from sys.syssequences "
				+(sequenceNamePattern!=null?"where sequencename = '"+sequenceNamePattern+"' ":"")
				+"order by sequencename ";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Sequence s = new Sequence();
			s.setSchemaName( schemaPattern );
			s.setName( rs.getString(1) );
			s.setMinValue(rs.getLong(2));
			s.setMaxValue(rs.getLong(3));
			s.setLastNumber(rs.getLong(4));
			s.setIncrementBy(rs.getLong(5));
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
		//log.debug("sql: "+query);
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
			String aliasInfo = rs.getString(5);
			
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
			eo.setBody(body);
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
	
	/*
	 * XXX: implement explainPlan?
	 * http://stackoverflow.com/questions/5981406/apache-derby-explain-select
	 */
}
