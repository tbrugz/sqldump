package tbrugz.sqldump.util;

public class SQLIdentifierDecorator extends StringDecorator {

	@Deprecated public static transient boolean dumpQuoteAll = false;
	@Deprecated public static transient String dumpIdentifierQuoteString = "\"";

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
