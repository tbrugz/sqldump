package tbrugz.sqldump.util;

public class StringDecorator {
	public String get(String str) { return str; }
	
	static StringDecorator instance = new StringDecorator();
	public static StringDecorator getInstance() { return instance; }
}
