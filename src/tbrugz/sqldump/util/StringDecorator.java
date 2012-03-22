package tbrugz.sqldump.util;

public class StringDecorator {

	public static class StringToLowerDecorator extends StringDecorator {
		@Override
		public String get(String str) {
			return str==null?null:str.toLowerCase();
		}
	}

	public static class StringToUpperDecorator extends StringDecorator {
		@Override
		public String get(String str) {
			return str==null?null:str.toUpperCase();
		}
	}
	
	public String get(String str) { return str; }
	
	static StringDecorator instance = new StringDecorator();
	public static StringDecorator getInstance() { return instance; }
}
