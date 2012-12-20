package tbrugz.sqldump.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//XXX: make StringDecorators singletons?
public class StringDecorator {
	
	static final Log log = LogFactory.getLog(StringDecorator.class);

	public final static String tolower = "tolower";
	public final static String toupper = "toupper";

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
	
	public static class StringQuoterDecorator extends StringDecorator {
		final String quote;
		
		public StringQuoterDecorator(String quote) {
			this.quote = quote;
		}
		
		@Override
		public String get(String str) {
			return str==null?null:quote+str+quote;
		}
	}
	
	
	public String get(String str) { return str; }
	
	static StringDecorator instance = new StringDecorator();
	public static StringDecorator getInstance() { return instance; }
	
	//TODO: make StringDecorators(upper/lower) singletons?
	public static StringDecorator getDecorator(String id) {
		if(tolower.equals(id)) { return new StringDecorator.StringToLowerDecorator(); }
		else if(toupper.equals(id)) { return new StringDecorator.StringToUpperDecorator(); }
		else if(id==null) {}
		else {
			log.warn("unknown decorator: "+id);
		}
		return getInstance();
	}	
}
