package tbrugz.sqldump.sqlrun.tokenzr;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NoSplitTokenizer implements Tokenizer, Iterator<String>, Iterable<String> {

	static final Log log = LogFactory.getLog(NoSplitTokenizer.class);

	final String[] stmtTokenizer;
	final boolean split;
	final int length;
	int pos = 0;
	
	public NoSplitTokenizer(String contents) {
		this(contents, true);
	}
	
	public NoSplitTokenizer(String contents, boolean split) {
		this.split = split;
		stmtTokenizer = new String[]{ contents };
		length = stmtTokenizer.length;
	}
	
	@Override
	public Iterator<String> iterator() {
		NoSplitTokenizer nst = new NoSplitTokenizer(stmtTokenizer[0], split);
		return nst;
	}

	@Override
	public boolean hasNext() {
		return pos<length;
	}

	@Override
	public String next() {
		if(hasNext()) {
			return stmtTokenizer[pos++];
		}
		throw new NoSuchElementException("pos = "+pos);
	}

	@Override
	public void remove() {
		log.warn("remove(): not implemented");
	}

}
