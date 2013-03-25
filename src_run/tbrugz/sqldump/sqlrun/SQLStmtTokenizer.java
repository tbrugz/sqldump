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
		
		if(aposPos==-1) {
			if(semicolonPos==-1) {
				String ret = sql.substring(pos);
				pos = sql.length();
				searchFrom = pos;
				return ret;
			}
			String ret = sql.substring(pos, semicolonPos);
			pos = semicolonPos+1;
			searchFrom = pos;
			return ret;
		}
		else if(semicolonPos<aposPos) {
			String ret = sql.substring(pos, semicolonPos);
			pos = semicolonPos+1;
			searchFrom = pos+1;
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
