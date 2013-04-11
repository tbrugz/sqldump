package tbrugz.sqldump.sqlrun;

import java.io.BufferedInputStream;
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
	
	public SQLStmtScanner(String file) throws FileNotFoundException {
		is = new BufferedInputStream(new FileInputStream(file));
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
		return scan.next();
	}

	@Override
	public void remove() {
	}
}
