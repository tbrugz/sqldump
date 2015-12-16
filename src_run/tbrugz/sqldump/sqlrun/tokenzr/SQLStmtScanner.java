package tbrugz.sqldump.sqlrun.tokenzr;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Scanner;

import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.sqlrun.AbstractTokenizer;

public class SQLStmtScanner implements AbstractTokenizer, Iterator<String>, Iterable<String> {

	final static String DEFAULT_CHARSET = DataDumpUtils.CHARSET_UTF8;
	//TODOne: option to define inputEncoding
	//XXX: option to define recordDelimiter?
	static final String recordDelimiter = ";";

	final boolean escapeBackslashApos; //mysql uses this escape - default is false
	final String inputEncoding;
	final Scanner scan;
	final InputStream is;
	
	/*public SQLStmtScanner(File file) throws FileNotFoundException {
		this(file, DEFAULT_CHARSET);
	}*/

	public SQLStmtScanner(File file, String charset, boolean escapeBackslashApos) throws FileNotFoundException {
		this(new BufferedInputStream(new FileInputStream(file)), charset, escapeBackslashApos);
	}
	
	SQLStmtScanner(InputStream is, String charset, boolean escapeBackslashApos) {
		this.is = is;
		this.inputEncoding = charset;
		this.escapeBackslashApos= escapeBackslashApos; 
		scan = new Scanner(is, inputEncoding);
		scan.useDelimiter(recordDelimiter);
	}

	/*public SQLStmtScanner(InputStream is) {
		this(is, DEFAULT_CHARSET);
	}*/
	
	public SQLStmtScanner(String string) {
		this(new ByteArrayInputStream(string.getBytes()), DEFAULT_CHARSET, false);
	}
	
	@Override
	public Iterator<String> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		return scan.hasNext();
	}

	@Override
	public String next() {
		//skipComments();
		String token = scan.next();
		int countApos = countApos(token);
		StringBuilder sb = new StringBuilder();
		sb.append(token);
		while (countApos>0 && countApos%2!=0) {
			if(!scan.hasNext()) {
				//XXX maybe an error in the imput file? log.debug() ?
				break;
			}
			token = scan.next();
			countApos += countApos(token);
			sb.append(token);
		}
		return sb.toString();
	}

	@Override
	public void remove() {
	}
	
	//TODO: ignore apos inside comments: "--;\n", "/*;*/" - if the comments are not inside strings...
	int countApos(String str) {
		int occurences = -1;
		int fromIndex = 0;
		do {
			int index = str.indexOf('\'', fromIndex);
			occurences++;
			//test for "\'" &apos; escapes
			if(escapeBackslashApos && index>=0 && str.charAt(index-1)=='\\') {
				occurences--;
			}
			fromIndex = index+1;
		} while(fromIndex>0);
		return occurences;
	}
	
	/*static final Pattern commentLine = Pattern.compile("--.*\n");
	static final Pattern commentBlock = Pattern.compile("/\\*[^\\* /]*\\* /");
	
	void skipComments() {
		try {
		scan.skip(commentLine);
		}
		catch(NoSuchElementException e) {}
		try {
		scan.skip(commentBlock);
		}
		catch(NoSuchElementException e) {}
	}*/
}
