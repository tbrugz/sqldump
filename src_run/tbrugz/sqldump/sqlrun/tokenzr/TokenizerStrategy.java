package tbrugz.sqldump.sqlrun.tokenzr;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.IOUtil;

public enum TokenizerStrategy {
	
	STMT_TOKENIZER,
	STMT_SCANNER,
	STMT_SCANNER_NG,
	STRING_SPLITTER,
	;
	
	static final Log log = LogFactory.getLog(TokenizerStrategy.class);
	
	public static final String STMT_TOKENIZER_CLASS = "SQLStmtTokenizer";
	public static final String STRING_SPLITTER_CLASS = "StringSpliter";
	public static final String STMT_SCANNER_CLASS = "SQLStmtScanner";
	public static final String STMT_SCANNER_NG_CLASS = "SQLStmtNgScanner";
	
	public static final TokenizerStrategy DEFAULT_STRATEGY = STMT_TOKENIZER;
	
	public static TokenizerStrategy getTokenizerStrategy(String tokenizer) {
		if(tokenizer == null) {
			return TokenizerStrategy.DEFAULT_STRATEGY;
		}
		tokenizer = tokenizer.trim();

		if(STMT_TOKENIZER_CLASS.equals(tokenizer)) {
			log.info("using '"+tokenizer+"' tokenizer class");
			return TokenizerStrategy.STMT_TOKENIZER;
		}
		else if(STRING_SPLITTER_CLASS.equals(tokenizer)) {
			log.warn("using deprecated '"+tokenizer+"' tokenizer class");
			return TokenizerStrategy.STRING_SPLITTER;
		}
		else if(STMT_SCANNER_CLASS.equals(tokenizer)) {
			log.info("using '"+tokenizer+"' tokenizer class");
			return TokenizerStrategy.STMT_SCANNER;
		}
		else if(STMT_SCANNER_NG_CLASS.equals(tokenizer)) {
			log.info("using '"+tokenizer+"' tokenizer class");
			return TokenizerStrategy.STMT_SCANNER_NG;
		}
		else {
			throw new IllegalArgumentException("unknown string tokenizer class: "+tokenizer);
		}
	}
	
	public static Tokenizer getTokenizer(TokenizerStrategy tokenizerStrategy, File file, String inputEncoding, boolean escapeBackslashedApos, boolean split) throws IOException {
		switch(tokenizerStrategy) {
		case STMT_SCANNER_NG:
			//XXX option to define charset
			return new SQLStmtNgScanner(file, inputEncoding);
		case STMT_SCANNER:
			//XXX option to define charset
			return new SQLStmtScanner(file, inputEncoding, escapeBackslashedApos);
		default:
			FileReader reader = null;
			try {
				reader = new FileReader(file);
				String fileStr = IOUtil.readFromReader(reader);
				switch (tokenizerStrategy) {
				case STMT_TOKENIZER:
					return new SQLStmtTokenizer(fileStr);
				case STRING_SPLITTER:
					return new StringSpliter(fileStr, split);
				default:
					throw new IllegalStateException("unknown TokenizerStrategy: "+tokenizerStrategy);
				}
			}
			finally {
				if(reader!=null) { reader.close(); }
			}
		}
	}

	public static Tokenizer getDefaultTokenizer(File file, String inputEncoding) throws IOException {
		return getTokenizer(DEFAULT_STRATEGY, file, inputEncoding, false, true);
	}
	
}
