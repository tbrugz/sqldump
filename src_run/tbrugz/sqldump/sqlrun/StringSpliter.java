package tbrugz.sqldump.sqlrun;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Deprecated
public class StringSpliter implements AbstractTokenizer, Iterator<String>, Iterable<String> {

	static final Log log = LogFactory.getLog(StringSpliter.class);

	final String[] stmtTokenizer;
	final int length;
	int pos = 0;
	
	public StringSpliter(String fileStr) {
		this(fileStr, true);
	}
	
	public StringSpliter(String fileStr, boolean split) {
		if(split) {
			stmtTokenizer = fileStr.split(";");
		}
		else {
			String[] z = { fileStr };
			stmtTokenizer = z;
		}
		length = stmtTokenizer.length;
	}
	
	@Override
	public Iterator<String> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		return pos<length;
	}

	@Override
	public String next() {
		return stmtTokenizer[pos++];
	}

	@Override
	public void remove() {
		log.warn("remove(): not implemented");
	}
}
