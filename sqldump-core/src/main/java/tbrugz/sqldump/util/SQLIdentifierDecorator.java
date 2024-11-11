package tbrugz.sqldump.util;

public class SQLIdentifierDecorator extends StringDecorator {

	static final boolean DEFAULT_DUMP_ALL = false;
	static final String DEFAULT_ID_QUOTE = "\"";
	
	@Deprecated public static transient boolean dumpQuoteAll = DEFAULT_DUMP_ALL;
	@Deprecated public static transient String dumpIdentifierQuoteString = DEFAULT_ID_QUOTE;
	
	/*public static void reset() {
		dumpQuoteAll = DEFAULT_DUMP_ALL;
		dumpIdentifierQuoteString = DEFAULT_ID_QUOTE;
	}*/

	@Override
	public String get(String id) {
		return (dumpQuoteAll?dumpIdentifierQuoteString:"")+id+(dumpQuoteAll?dumpIdentifierQuoteString:"");
	}
	
	static SQLIdentifierDecorator instance = new SQLIdentifierDecorator();
	public static StringDecorator getInstance() { return instance; }
	
	@Override
	public String toString() {
		return "SQLIdentifierDecorator[quoteAll="+dumpQuoteAll+";dumpIdentifierQuoteString="+dumpIdentifierQuoteString+"]";
	}
}
