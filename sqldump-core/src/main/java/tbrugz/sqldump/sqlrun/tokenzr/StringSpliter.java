package tbrugz.sqldump.sqlrun.tokenzr;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Deprecated
public class StringSpliter implements Tokenizer, Iterator<String>, Iterable<String> {

	static final Log log = LogFactory.getLog(StringSpliter.class);

	final String[] stmtTokenizer;
	final int length;
	int pos = 0;
	boolean iteratorAlreadyReturned = false;
	
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
		if(iteratorAlreadyReturned) {
			throw new IllegalStateException("Iterator already returned");
		}
		iteratorAlreadyReturned = true;
		return this; // NOSONAR
	}

	@Override
	public boolean hasNext() {
		return pos<length;
	}

	@Override
	public String next() {
		if(!hasNext()) {
			throw new NoSuchElementException("pos = "+pos);
		}
		return stmtTokenizer[pos++];
	}

	@Override
	public void remove() {
		log.warn("remove(): not implemented");
	}
}
