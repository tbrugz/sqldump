package tbrugz.sqldump.processors;

import java.io.IOException;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.Query;
import tbrugz.sqldump.dbmodel.Relation;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.View;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.StringDecorator;
import tbrugz.sqldump.util.Utils;

/**
 * Processor that validates a Schema Model.
 */
/*
 * XXX: option to remove invalid objects
 * XXX: option to fail if invalid objects found
 */
public class ModelValidator extends AbstractSQLProc {
	
	static final Log log = LogFactory.getLog(ModelValidator.class);
	
	StringDecorator sqlIdDecorator = null;
	
	Writer wout;

	@Override
	public void process() {
		try {
			doIt();
		} catch (SQLException e) {
			log.warn("SQLException: "+e);
			if(failonerror) {
				throw new ProcessingException(e);
			}
		} catch (IOException e) {
			log.warn("IOException: "+e);
			log.debug("IOException: "+e.getMessage(), e);
			if(failonerror) {
				throw new ProcessingException(e);
			}
		}
	}

	void doIt() throws IOException, SQLException {
		
		int relationsCount = 0, relationsValidated = 0;
		
		DBMSFeatures feat = DBMSResources.instance().getSpecificFeatures(conn.getMetaData());
		sqlIdDecorator = new StringDecorator.StringQuoterDecorator(feat.getIdentifierQuoteString());
		
		// tables
		for(Table t: model.getTables()) {
			try {
				validateRelation(t);
				relationsValidated++;
			} catch (SQLException e) {
				out("error validating table '"+t.getQualifiedName()+"'");
				//log.warn("ex: "+e);
			}
			relationsCount++;
		}
		
		// views
		for(View v: model.getViews()) {
			try {
				if(v instanceof Query) {
					Query q = (Query) v;
					validateSQL(q.getQuery());
				}
				else {
					validateRelation(v);
				}
				relationsValidated++;
			}
			catch(SQLException e) {
				out("error validating view '"+v.getQualifiedName()+"'");
				//log.warn("ex: "+e);
			}
			relationsCount++;
		}
		
		//XXX validate executables, fks, triggers, sequences, synonyms, indexes ?
		
		String message = relationsValidated+" relations validaded [of "+relationsCount+" processed]";
		out(message);
		
		if(relationsValidated < relationsCount) {
			// throw exception if fail on error?
		}
	}
	
	void validateRelation(Relation r) throws SQLException {
		List<String> cols = r.getColumnNames();
		String colsStr = "*";
		if(cols!=null && cols.size()>0) {
			colsStr = Utils.join(cols, ", ", sqlIdDecorator); 
		}
		String sql = "select "+colsStr+" from "+DBObject.getFinalName(r, sqlIdDecorator, true);
		validateSQL(sql);
	}
	
	void validateSQL(String sql) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(sql);
		//ResultSetMetaData rsmd = ps.getMetaData();
		ps.getMetaData();
	}
	
	void out(String s) throws IOException {
		if(wout!=null) {
			wout.write(s+"\n");
		}
		else {
			log.info(s);
		}
	}
	
	@Override
	public boolean acceptsOutputWriter() {
		return true;
	}
	
	@Override
	public void setOutputWriter(Writer writer) {
		this.wout = writer;
	}
	
}
