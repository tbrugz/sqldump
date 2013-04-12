package tbrugz.sqldump.sqlrun;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Scanner;

public class SQLStmtScanner implements Iterator<String>, Iterable<String> {

	//final static String DEFAULT_CHARSET = DataDumpUtils.CHARSET_UTF8;
	//TODOne: option to define inputEncoding
	//XXX: option to define recordDelimiter?
	final static String recordDelimiter = ";";
	final String inputEncoding;
	final Scanner scan;
	final InputStream is;
	
	/*public SQLStmtScanner(File file) throws FileNotFoundException {
		this(file, DEFAULT_CHARSET);
	}*/

	public SQLStmtScanner(File file, String charset) throws FileNotFoundException {
		this(new BufferedInputStream(new FileInputStream(file)), charset);
	}
	
	public SQLStmtScanner(InputStream is, String charset) {
		this.is = is;
		inputEncoding = charset;
		scan = new Scanner(is, inputEncoding);
		scan.useDelimiter(recordDelimiter);
	}

	/*public SQLStmtScanner(InputStream is) {
		this(is, DEFAULT_CHARSET);
	}*/
	
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
		String token = scan.next();
		int countApos = countApos(token);
		StringBuilder sb = new StringBuilder();
		sb.append(token);
		while (countApos%2==1) {
			token = scan.next();
			countApos += countApos(token);
			sb.append(token);
		}
		return sb.toString();
	}

	@Override
	public void remove() {
	}
	
	//TODO: ignore apos inside comments: "--;\n", "/*;*/" - if they are not inside strings...
	static int countApos(String str) {
		int occurences = -1;
		int fromIndex = 0;
		do {
			int index = str.indexOf('\'', fromIndex);
			fromIndex = index+1;
			occurences++;
		} while(fromIndex>0);
		return occurences;
	}
}
