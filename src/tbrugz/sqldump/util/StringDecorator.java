package tbrugz.sqldump.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StringDecorator {
	
	static final Log log = LogFactory.getLog(StringDecorator.class);

	final static String tolower = "tolower";
	final static String toupper = "toupper";

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
	
	public static StringDecorator getDecorator(String id) {
		if(tolower.equals(id)) { return new StringDecorator.StringToLowerDecorator(); }
		else if(toupper.equals(id)) { return new StringDecorator.StringToUpperDecorator(); }
		
		log.warn("unknown decorator: "+id);
		return new StringDecorator();
	}	
}
