package tbrugz.sqldump.util;

public class SQLIdentifierDecorator extends StringDecorator {

	public static transient boolean dumpQuoteAll = false;
	public static transient String dumpIdentifierQuoteString = "\"";

	@Override
	public String get(String id) {
		return (dumpQuoteAll?dumpIdentifierQuoteString:"")+id+(dumpQuoteAll?dumpIdentifierQuoteString:"");
	}
	
	static SQLIdentifierDecorator instance = new SQLIdentifierDecorator();
	public static StringDecorator getInstance() { return instance; }
}
