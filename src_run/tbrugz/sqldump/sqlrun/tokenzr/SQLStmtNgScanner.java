package tbrugz.sqldump.sqlrun.tokenzr;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.datadump.DataDumpUtils;

public class SQLStmtNgScanner implements Tokenizer, Iterator<String>, Iterable<String> {

	static final Log log = LogFactory.getLog(SQLStmtNgScanner.class);
	
	final static String DEFAULT_CHARSET = DataDumpUtils.CHARSET_UTF8;

	//XXX: option to define recordDelimiter?
	static final String recordDelimiter = ";";
	
	//static final Pattern COMMENT = Pattern.compile("--.*?\n");
	static final String COMMENT_INI = "--";
	static final String COMMENT_END = "\n";
	
	//static final Pattern COMMENT_BLOCK = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
	static final String COMMENT_BLOCK_INI = "/*";
	static final String COMMENT_BLOCK_END = "*/";

	//static final Pattern STRING = Pattern.compile("'.*?'");
	static final String STRING_INI = "'";
	static final String STRING_END = "'";
	
	final String inputEncoding;
	final Scanner scan;
	final InputStream is;
	
	public SQLStmtNgScanner(File file, String charset) throws FileNotFoundException {
		this(new BufferedInputStream(new FileInputStream(file)), charset);
	}
	
	SQLStmtNgScanner(InputStream is, String charset) {
		this.is = is;
		this.inputEncoding = charset;
		scan = new Scanner(is, inputEncoding);
		scan.useDelimiter(recordDelimiter);
	}

	public SQLStmtNgScanner(String string) {
		this(new ByteArrayInputStream(string.getBytes()), DEFAULT_CHARSET);
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
		String tokenz = scan.next();
		StringBuilder sb = new StringBuilder();
		sb.append(tokenz);
		
		boolean doBreak = true;
		
		do {
			doBreak = true;
			tokenz = sb.toString();
			log.info("token: "+tokenz);
			
			int iniApos = tokenz.indexOf(STRING_INI);
			int iniBlComment = tokenz.indexOf(COMMENT_BLOCK_INI);
			int iniComment = tokenz.indexOf(COMMENT_INI);
			int endApos = -1;
			int endBlComment = -1;
			int endComment = -1;
			
			if(iniApos!=-1) { endApos = tokenz.indexOf(STRING_END, iniApos+1); }
			if(iniBlComment!=-1) { endBlComment = tokenz.indexOf(COMMENT_BLOCK_END, iniBlComment+1); }
			if(iniComment!=-1) { endComment = tokenz.indexOf(COMMENT_END, iniComment+1); }
			
			if(iniApos==-1) { iniApos = Integer.MAX_VALUE; }
			if(iniBlComment==-1) { iniBlComment = Integer.MAX_VALUE; }
			if(iniComment==-1) { iniComment = Integer.MAX_VALUE; }
			
			/*
			log.info("iniApos: "+iniApos+"; endApos: "+endApos
					+"; iniBlComment: "+iniBlComment+"; endBlComment: "+endBlComment
					+"; iniComment: "+iniComment+"; endComment: "+endComment);
			*/
			
			if(iniApos<iniBlComment && iniApos<iniComment && endApos==-1) {
				if(!scan.hasNext()) { break; }
				tokenz = scan.next();
				sb.append(recordDelimiter+tokenz);
				doBreak = false;
			}
			if(iniBlComment<iniComment && endBlComment==-1) {
				if(!scan.hasNext()) { break; }
				tokenz = scan.next();
				sb.append(recordDelimiter+tokenz);
				doBreak = false;
			}
			if(iniComment<Integer.MAX_VALUE && endComment==-1) {
				if(!scan.hasNext()) { break; }
				tokenz = scan.next();
				sb.append(recordDelimiter+tokenz);
				doBreak = false;
			}
			/*else {
				doBreak = true;
			}*/
		} while(!doBreak);

		return sb.toString();
	}

	@Override
	public void remove() {
	}
}
