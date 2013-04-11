package tbrugz.sqldump.sqlrun;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Scanner;

import tbrugz.sqldump.datadump.DataDumpUtils;

public class SQLStmtScanner implements Iterator<String>, Iterable<String> {

	//TODO: option to define recordDelimiter & inputEncoding
	final static String recordDelimiter = ";";
	final static String inputEncoding = DataDumpUtils.CHARSET_UTF8;
	final Scanner scan;
	final InputStream is;
	
	public SQLStmtScanner(File file) throws FileNotFoundException {
		is = new BufferedInputStream(new FileInputStream(file));
		scan = new Scanner(is, inputEncoding);
		scan.useDelimiter(recordDelimiter);
	}

	public SQLStmtScanner(InputStream is) {
		this.is = is;
		scan = new Scanner(is, inputEncoding);
		scan.useDelimiter(recordDelimiter);
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
