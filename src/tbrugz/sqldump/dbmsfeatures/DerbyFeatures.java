package tbrugz.sqldump.dbmsfeatures;

import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

import tbrugz.sqldump.DefaultDBMSFeatures;
import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.View;

public class DerbyFeatures extends DefaultDBMSFeatures {
	static Logger log = Logger.getLogger(DerbyFeatures.class);

	public void procProperties(Properties prop) {
	}

	public void grabDBObjects(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		grabDBViews(model, schemaPattern, conn);
		grabDBTriggers(model, schemaPattern, conn);
		
		try {
			grabDBSequences(model, schemaPattern, conn);
		}
		catch(SQLSyntaxErrorException e) {
			log.warn("can't grab derby sequences. database version 10.6+ required"); //XXX output current db version?
			log.debug("nested exception: "+e);
		}
		
		//XXX: add procedures/functions? synonyms?
	}

	void grabDBViews(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing views");
		//String query = "select tableid, viewdefinition "
		//		+"from sys.sysviews "
		//		+"order by tableid ";
		String query = "select st.tablename, sv.tableid, sv.viewdefinition "
				+"from sys.systables as st, sys.sysviews as sv "
				+"where st.tableid = sv.tableid "
				+"order by tablename ";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			View v = new DerbyView();
			v.name = rs.getString(1);
			//v.query = getStringFromReader(rs.getCharacterStream(3));
			v.query = rs.getString(3);
			v.schemaName = schemaPattern;
			model.getViews().add(v);
			count++;
		}
		
		log.info(count+" views grabbed");
	}

	void grabDBTriggers(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing triggers");
		String query = "select triggername, event, firingtime, type, st.tableid, tablename, triggerdefinition "
				+"from sys.systables as st, sys.systriggers as tr "
				+"where st.tableid = tr.tableid  ";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		InformationSchemaTrigger.addSplitter = true;
		
		int count = 0;
		while(rs.next()) {
			InformationSchemaTrigger t = new InformationSchemaTrigger();
			//t.addSplitter = true;
			t.name = rs.getString(1);
			t.schemaName = schemaPattern;
			//t.tableName = rs.getString(3);
			//t.description = rs.getString(4);
			//t.description = t.schemaName+"."+t.name+" as";
			//t.body = rs.getString(2);
			
			//XXX: derby REFERENCING?
			
			//t.schemaName = rs.getString(2);
			//t.name = rs.getString(1);
			String event = rs.getString(2);
			t.eventsManipulation.add("I".equals(event)?"INSERT":"U".equals(event)?"UPDATE":"DELETE");
			t.tableName = rs.getString(6);
			t.actionStatement = rs.getString(7);
			String forEachX = rs.getString(4);
			t.actionOrientation = "R".equals(forEachX)?"ROW":"STATEMENT";
			String timing = rs.getString(3);
			t.conditionTiming = "A".equals(timing)?"AFTER":"BEFORE";
			
			model.getTriggers().add(t);
			count++;
		}
		
		log.info(count+" triggers grabbed");
	}
	
	void grabDBSequences(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing sequences");
		String query = "select sequencename, minimumvalue, maximumvalue, currentvalue, increment, sequencedatatype "
				+"from sys.syssequences "
				+"order by sequencename ";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Sequence s = new Sequence();
			s.schemaName = schemaPattern;
			s.name = rs.getString(1);
			s.minValue = rs.getLong(2);
			s.maxValue = rs.getLong(3);
			s.lastNumber = rs.getLong(4);
			s.incrementBy = rs.getLong(5);
			model.getSequences().add(s);
			count++;
		}
		
		log.info(count+" sequences grabbed");
	}

	//not used...
	static String getStringFromReader(Reader r) {
		CharBuffer cb = CharBuffer.allocate(2000);
		try {
			r.read(cb);
		}
		catch(IOException ioe) {
			log.warn(ioe);
		}
		int lenght = cb.position();
		cb.position(0);
		return cb.subSequence(0, lenght).toString();
	} 
}
