package tbrugz.sqldump.sqlrun;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SQLStmtTokenizer implements Iterator<String>, Iterable<String> {

	static final Log log = LogFactory.getLog(SQLStmtTokenizer.class);
	
	final String sql;
	final int length;
	
	int pos = 0;
	int searchFrom = 0;
	
	public SQLStmtTokenizer(String sql) {
		this.sql = sql;
		length = sql.length();
	}
	
	@Override
	public boolean hasNext() {
		return !(pos==length);
	}

	@Override
	public String next() {
		if(!hasNext()) { return null; }
		
		int aposPos = sql.indexOf("'", searchFrom);
		int semicolonPos = sql.indexOf(";", searchFrom);
		int commPos = sql.indexOf("--", searchFrom);

		//log.info("aposPos: "+aposPos+"; semicolonPos: "+semicolonPos+"; commPos:"+commPos); 
		
		if(aposPos==-1) {
			if(semicolonPos==-1) {
				String ret = sql.substring(pos);
				pos = sql.length();
				searchFrom = pos;
				return ret;
			}
			int endPos = semicolonPos;
			if(commPos>=0 && commPos<semicolonPos) {
				int nlPos = sql.indexOf("\n", commPos);
				endPos = nlPos;
				//log.info("nlPos: "+nlPos+";"); 
			}
			String ret = sql.substring(pos, endPos);
			pos = endPos+1;
			searchFrom = pos;
			return ret;
		}
		else if(semicolonPos<aposPos) {
			int endPos = semicolonPos;
			if(commPos>=0 && commPos<semicolonPos) {
				int nlPos = sql.indexOf("\n", commPos);
				endPos = nlPos;
			}
			String ret = sql.substring(pos, endPos);
			pos = endPos+1;
			searchFrom = pos;
			return ret;
		}
		else {
			searchFrom = sql.indexOf("'", aposPos+1)+1;
			return next();
		}
	}

	@Override
	public void remove() {
		log.warn("remove(): not implemented");
	}

	@Override
	public Iterator<String> iterator() {
		return this;
	}
}
