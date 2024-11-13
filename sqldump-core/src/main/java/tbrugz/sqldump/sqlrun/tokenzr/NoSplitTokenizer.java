package tbrugz.sqldump.sqlrun.tokenzr;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NoSplitTokenizer implements Tokenizer, Iterator<String>, Iterable<String> {

	static final Log log = LogFactory.getLog(NoSplitTokenizer.class);

	final String[] stmtTokenizer;
	final int length;
	int pos = 0;
	
	public NoSplitTokenizer(String contents) {
		this(contents, true);
	}
	
	public NoSplitTokenizer(String contents, boolean split) {
		stmtTokenizer = new String[]{ contents };
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
