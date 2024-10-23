package tbrugz.sqldump.sqlrun.tokenzr;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.IOUtil;

public enum TokenizerStrategy {
	
	STMT_TOKENIZER,
	STMT_SCANNER,
	STMT_SCANNER_NG,
	STMT_PARSER_TOKENIZER,
	STRING_SPLITTER,
	//NO_TOKENIZER, //??
	;
	
	static final Log log = LogFactory.getLog(TokenizerStrategy.class);
	
	public static final String STMT_TOKENIZER_CLASS = "SQLStmtTokenizer";
	public static final String STRING_SPLITTER_CLASS = "StringSpliter";
	public static final String STMT_SCANNER_CLASS = "SQLStmtScanner";
	public static final String STMT_SCANNER_NG_CLASS = "SQLStmtNgScanner";
	public static final String STMT_PARSER_TOKENIZER_CLASS = "SQLStmtParserTokenizer";
	//public static final String NO_TOKENIZER_CLASS = "NoSplitTokenizer";
	
	public static final TokenizerStrategy DEFAULT_STRATEGY = STMT_PARSER_TOKENIZER;
	
	public static TokenizerStrategy getTokenizerStrategy(String tokenizer) {
		if(tokenizer == null) {
			TokenizerStrategy ret = TokenizerStrategy.DEFAULT_STRATEGY;
			//log.debug("using default '"+ret+"' tokenizer strategy");
			return ret;
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
		else if(STMT_PARSER_TOKENIZER_CLASS.equals(tokenizer)) {
			log.info("using '"+tokenizer+"' tokenizer class");
			return TokenizerStrategy.STMT_PARSER_TOKENIZER;
		}
		/*else if(NO_TOKENIZER_CLASS.equals(tokenizer)) {
			log.info("using '"+tokenizer+"' (NO) tokenizer class");
			return TokenizerStrategy.NO_TOKENIZER;
		}*/
		else {
			throw new IllegalArgumentException("unknown string tokenizer class: "+tokenizer);
		}
	}
	
	@SuppressWarnings("deprecation")
	public static Tokenizer getTokenizer(TokenizerStrategy tokenizerStrategy, File file, String inputEncoding, boolean escapeBackslashedApos, boolean split) throws IOException {
		//log.debug("getTokenizer: strategy="+tokenizerStrategy+" ; charset = "+inputEncoding);
		if(!split) {
			String fileStr = IOUtil.readFromFile(file, inputEncoding, true);
			return new NoSplitTokenizer(fileStr);
		}

		switch(tokenizerStrategy) {
		case STMT_SCANNER_NG:
			return new SQLStmtNgScanner(file, inputEncoding);
		case STMT_PARSER_TOKENIZER:
			return new SQLStmtParserTokenizer(file, inputEncoding);
		case STMT_SCANNER:
			return new SQLStmtScanner(file, inputEncoding, escapeBackslashedApos);
		default:
			// https://stackoverflow.com/questions/696626/java-filereader-encoding-issue
			//Reader reader = null;
			//try {
				//reader = new InputStreamReader(new FileInputStream(file), inputEncoding);
				//String fileStr = IOUtil.readFromReader(reader);
				String fileStr = IOUtil.readFromFile(file, inputEncoding, true);
				switch (tokenizerStrategy) {
				case STMT_TOKENIZER:
					return new SQLStmtTokenizer(fileStr);
				case STRING_SPLITTER:
					log.warn("Strategy "+tokenizerStrategy+" is deprecated");
					return new StringSpliter(fileStr);
				//case NO_TOKENIZER:
				//	return new NoSplitTokenizer(fileStr);
				default:
					throw new IllegalStateException("unknown TokenizerStrategy: "+tokenizerStrategy);
				}
			//}
			//finally {
			//	if(reader!=null) { reader.close(); }
			//}
		}
	}

	public static Tokenizer getDefaultTokenizer(File file, String inputEncoding) throws IOException {
		return getTokenizer(DEFAULT_STRATEGY, file, inputEncoding, false, true);
	}
	
}
