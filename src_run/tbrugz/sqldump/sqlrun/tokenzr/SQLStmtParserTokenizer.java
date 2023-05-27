package tbrugz.sqldump.sqlrun.tokenzr;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

import tbrugz.sqldump.sqlrun.tokenzr.TokenizerUtil.TknState;

public class SQLStmtParserTokenizer implements Tokenizer, Iterator<String>, Iterable<String> {

	/*enum TknState {
		DEFAULT, STRING, DQUOTE, COMM_LINE, COMM_BLOCK
	}*/

	//final static String DEFAULT_CHARSET = DataDumpUtils.CHARSET_UTF8;

	//final String inputEncoding;
	final InputStreamReader isr;
	String nextToken;

	public SQLStmtParserTokenizer(File file, String charset) throws FileNotFoundException, UnsupportedEncodingException {
		this(new BufferedInputStream(new FileInputStream(file)), charset);
	}

	SQLStmtParserTokenizer(InputStream is, String charset) {
		//this.inputEncoding = charset;
		try {
			isr = new InputStreamReader(is, charset);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public SQLStmtParserTokenizer(String string, String charset) {
		this(new ByteArrayInputStream(string.getBytes()), charset);
	}

	public static String getNextToken(InputStreamReader isr) {
		TknState state = TknState.DEFAULT;
		StringBuilder ret = new StringBuilder();

		int chi = -1;
		try {
			chi = isr.read();
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}

		char prevc = 0;
		while_loop:
		while(chi != -1) {
			char c = (char) chi;
			//boolean lastChar = (chi == -1);
			switch (state) {
				case DEFAULT: {
					if(c=='\'') {
						state = TknState.STRING;
						ret.append(c);
					}
					else if(c=='"') {
						state = TknState.DQUOTE;
						ret.append(c);
					}
					else if(c=='-' && prevc=='-') {
						state = TknState.COMM_LINE;
						ret.append(c);
					}
					else if(c=='*' && prevc=='/') {
						state = TknState.COMM_BLOCK;
						ret.append(c);
					}
					else if(c==';') {
						// end loop
						break while_loop;
					}
					else {
						ret.append(c);
					}
					break;
				}
				case STRING: {
					if(c=='\'') {
						state = TknState.DEFAULT;
					}
					ret.append(c);
					break;
				}
				case DQUOTE: {
					if(c=='"') {
						state = TknState.DEFAULT;
					}
					ret.append(c);
					break;
				}
				case COMM_LINE: {
					if(c=='\n') {
						state = TknState.DEFAULT;
					}
					ret.append(c);
					break;
				}
				case COMM_BLOCK: {
					if(c=='/' && prevc=='*') {
						state = TknState.DEFAULT;
					}
					ret.append(c);
					break;
				}
			}
			prevc = c;
			//if(lastChar) { break; }
			try {
				chi = isr.read();
			}
			catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
	
		//String sret = ret.toString();
		//System.out.println("["+sret+"]");
		if(ret.isEmpty()) { return null; }
		return ret.toString();
	}

	@Override
	public Iterator<String> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		if(nextToken==null) {
			nextToken = getNextToken(isr);
		}
		return nextToken!=null;
	}

	@Override
	public String next() {
		if(nextToken==null) {
			nextToken = getNextToken(isr);
		}
		String ret = nextToken;
		nextToken = null;
		return ret;
	}

}
