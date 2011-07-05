package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

import tbrugz.sqldump.DBMSFeatures;
import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.Utils;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.View;

public class InformationSchemaFeatures implements DBMSFeatures {
	static Logger log = Logger.getLogger(InformationSchemaFeatures.class);

	boolean grabIndexes = false;
	//boolean dumpSequenceStartWith = true;
	
	public void procProperties(Properties prop) {
		grabIndexes = "true".equals(prop.getProperty(PROP_GRAB_INDEXES));
		//Sequence.dumpStartWith = "true".equals(prop.getProperty(PROP_SEQUENCE_STARTWITHDUMP));
	}
	
	public void grabDBObjects(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		grabDBViews(model, schemaPattern, conn);
		//grabDBTriggers(model, schemaPattern, conn);
		grabDBRoutines(model, schemaPattern, conn);
		//grabDBSynonyms(model, schemaPattern, conn);
		if(grabIndexes) {
			//grabDBIndexes(model, schemaPattern, conn);
		}
		grabDBSequences(model, schemaPattern, conn);
	}
	
	void grabDBViews(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing views");
		String query = "select table_catalog, table_schema, table_name, view_definition from information_schema.views "
				+"where view_definition is not null";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			View v = new View();
			v.name = rs.getString(3);
			v.query = rs.getString(4);
			v.query = v.query.substring(0, v.query.length()-2);
			v.schemaName = schemaPattern;
			model.getViews().add(v);
			count++;
		}
		
		log.info(count+" views grabbed");// ["+model.views.size()+"/"+count+"]: ");
	}

	/*
	public void grabDBTriggers(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing triggers");
		String query = "SELECT TRIGGER_NAME, TABLE_OWNER, TABLE_NAME, DESCRIPTION, TRIGGER_BODY FROM USER_TRIGGERS";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Trigger t = new Trigger();
			t.name = rs.getString(1);
			t.schemaName = rs.getString(2);
			t.tableName = rs.getString(3);
			t.description = rs.getString(4);
			t.body = rs.getString(5);
			model.triggers.add(t);
			count++;
		}
		
		log.info(model.triggers.size()+" triggers grabbed");
	}
	*/

	void grabDBRoutines(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing executables");
		String query = "select routine_name, routine_type, data_type, external_language, routine_definition "
				+"from information_schema.routines "
				+"where routine_definition is not null ";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			InformationSchemaRoutine eo = new InformationSchemaRoutine();
			eo.schemaName = schemaPattern;
			eo.name = rs.getString(1);
			try {
				eo.type = DBObjectType.valueOf(Utils.normalizeEnumStringConstant(rs.getString(2)));
			}
			catch(IllegalArgumentException iae) {
				log.warn("unknown object type: "+rs.getString(2));
				eo.type = DBObjectType.EXECUTABLE;
			}
			eo.returnType = rs.getString(3);
			eo.externalLanguage = rs.getString(4);
			eo.body = rs.getString(5);
			model.getExecutables().add(eo);
			count++;
		}
		
		log.info(count+" executable objects/routines grabbed");
	}

	/*
	public void grabDBSynonyms(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing synonyms");
		String query = "select synonym_name, table_owner, table_name, db_link from user_synonyms";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Synonym s = new Synonym();
			s.name = rs.getString(1);
			s.schemaName = schemaPattern;
			s.objectOwner = rs.getString(2);
			s.referencedObject = rs.getString(3);
			s.dbLink = rs.getString(4);
			model.synonyms.add(s);
			count++;
		}
		
		log.info(model.synonyms.size()+" synonyms grabbed");
	}*/

	void grabDBSequences(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing sequences");
		String query = "select sequence_name, minimum_value, increment, maximum_value from information_schema.sequences order by sequence_name"; // increment_by, last_number? 
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Sequence s = new Sequence();
			s.schemaName = schemaPattern;
			s.name = rs.getString(1);
			s.minValue = rs.getLong(2);
			s.incrementBy = 1; //rs.getLong(3);
			//s.lastNumber = rs.getLong(4);
			model.getSequences().add(s);
			count++;
		}
		
		log.info(count+" sequences grabbed");
	}
	
}
