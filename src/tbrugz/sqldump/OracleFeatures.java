package tbrugz.sqldump;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.Synonym;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;

public class OracleFeatures implements DBMSFeatures {
	static Logger log = Logger.getLogger(OracleFeatures.class);

	/* (non-Javadoc)
	 * @see tbrugz.sqldump.DbmgrFeatures#grabDBObjects(tbrugz.sqldump.SchemaModel, java.lang.String, java.sql.Connection)
	 */
	public void grabDBObjects(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		grabDBViews(model, schemaPattern, conn);
		grabDBTriggers(model, schemaPattern, conn);
		grabDBExecutables(model, schemaPattern, conn);
		grabDBSynonyms(model, schemaPattern, conn);
	}
	
	public void grabDBViews(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		String query = "SELECT VIEW_NAME, TEXT FROM USER_VIEWS ORDER BY VIEW_NAME";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			View v = new View();
			v.name = rs.getString(1);
			v.query = rs.getString(2);
			v.schemaName = schemaPattern;
			model.views.add(v);
			count++;
		}
		
		log.info(model.views.size()+" views grabbed");// ["+model.views.size()+"/"+count+"]: ");
		
		//for(View v: model.views) {
			//System.out.println(v);
			//System.out.println(v.getDefinition(true));
		//}
		//SQLDataDump.dumpRS(rs);
	}

	public void grabDBTriggers(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		String query = "SELECT TRIGGER_NAME, TABLE_OWNER, DESCRIPTION, TRIGGER_BODY FROM USER_TRIGGERS";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Trigger t = new Trigger();
			t.name = rs.getString(1);
			t.schemaName = rs.getString(2);
			t.description = rs.getString(3);
			t.body = rs.getString(4);
			model.triggers.add(t);
			count++;
		}
		
		log.info(model.triggers.size()+" triggers grabbed");
	}

	public void grabDBExecutables(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		String query = "select name, type, line, text from user_source "
			+"where type in ('PROCEDURE','PACKAGE','PACKAGE BODY','FUNCTION','TYPE') "
			+"order by type, name, line";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		ExecutableObject eo = null;
		StringBuffer sb = null;
		
		while(rs.next()) {
			int line = rs.getInt(3);
			if(line==1) {
				//end last object
				if(eo!=null) {
					eo.body = sb.toString();
					model.executables.add(eo);
				}
				//new object
				eo = new ExecutableObject();
				sb = new StringBuffer();
				eo.name = rs.getString(1);
				eo.type = rs.getString(2);
				eo.schemaName = schemaPattern;
			}
			sb.append(rs.getString(4)); //+"\n"
			count++;
		}
		
		log.info(model.executables.size()+" executable objects grabbed");
	}

	public void grabDBSynonyms(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
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
	}
	
}
