package tbrugz.sqldump.sqlrun.tokenzr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
	
	public static TokenizerStrategy getTokenizer(String tokenizer) {
		if(tokenizer == null) {
			return TokenizerStrategy.STMT_SCANNER;
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
}
