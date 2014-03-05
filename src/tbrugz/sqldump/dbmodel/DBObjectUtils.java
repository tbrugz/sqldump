package tbrugz.sqldump.dbmodel;

public class DBObjectUtils {
	
	static final String NL = "\n";
	
	public static boolean equalsIgnoreWhitespacesEachLine(String s1, String s2) {
		if(s1==null && s2==null) { return true; }
		if(s1==null || s2==null) { return false; }
		
		String[] s1a = s1.split(NL);
		String[] s2a = s2.split(NL);
		
		if(s1a.length!=s2a.length) { return false; }
		
		for(int i=0;i<s1a.length;i++) {
			if(!equalsWithTrim(s1a[i], s2a[i])) { return false; }
		}
		return true;
	}
	
	public static boolean equalsWithTrim(String s1, String s2) {
		return s1.trim().equals(s2.trim());
	}

}
