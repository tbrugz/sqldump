package tbrugz.sqldump.sqlrun.tokenzr;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SQLStmtTokenizer implements Tokenizer, Iterator<String>, Iterable<String> {

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
		int commBlPos = sql.indexOf("/*", searchFrom);
		int skip = 1;

		//log.info("aposPos: "+aposPos+"; semicolonPos: "+semicolonPos+"; commPos:"+commPos+"; commBlPos:"+commBlPos+"; searchFrom: "+searchFrom); 
		
		if( (aposPos==-1) && (semicolonPos==-1) ) {
			String ret = sql.substring(pos);
			pos = sql.length();
			searchFrom = pos;
			return ret;
		}
		if( (aposPos==-1) || (semicolonPos<aposPos) ) {
			int endPos = semicolonPos;
			if(commPos>=0 && commPos<semicolonPos) {
				int nlPos = sql.indexOf("\n", commPos);
				//log.info("nlPos: "+nlPos);
				//if(nlPos>semicolonPos) {
					searchFrom = nlPos+1;
					return next();
				//}
				//endPos = nlPos;
				//skip = 2;
			}
			if(commBlPos>=0 && commBlPos<semicolonPos) {
				int commBlEndPos = sql.indexOf("*/", commBlPos);
				//log.info("commBlEndPos: "+commBlEndPos); 
				//if(commBlEndPos>semicolonPos) {
					searchFrom = commBlEndPos+2;
					return next();
				//}
				//endPos = semicolonPos;
				//skip = 0;
			}
			//log.info("endPos: "+endPos+" ;; pos:"+pos+" ;; sql="+sql );
			String ret = null;
			if(endPos<pos) {
				ret = sql.substring(pos);
				pos = sql.length();
			}
			else {
				ret = sql.substring(pos, endPos);
				pos = endPos+skip;
			}
			searchFrom = pos;
			return ret;
		}
		/*else if(semicolonPos<aposPos) {
			int endPos = semicolonPos;
			if(commPos>=0 && commPos<semicolonPos) {
				int nlPos = sql.indexOf("\n", commPos);
				endPos = nlPos;
				skip = 2;
			}
			if(commBlPos>=0 && commBlPos<semicolonPos) {
				int commBlEndPos = sql.indexOf("* /", commBlPos);
				log.info("commBlEndPos: "+commBlEndPos); 
				if(commBlEndPos+2<semicolonPos) {
					searchFrom = commBlEndPos+2;
					return next();
				}
				endPos = commBlEndPos+2;
				skip = 0;
			}
			String ret = sql.substring(pos, endPos);
			pos = endPos+skip;
			searchFrom = pos;
			return ret;
		}*/
		else {
			searchFrom = sql.indexOf("'", aposPos+1)+1;
			if(searchFrom==0) {
				String ret = sql.substring(pos);
				pos = sql.length();
				searchFrom = pos;
				return ret;
			}
			//log.info("; searchFrom: "+searchFrom); 
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
