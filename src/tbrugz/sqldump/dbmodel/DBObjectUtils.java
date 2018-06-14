package tbrugz.sqldump.dbmodel;

import tbrugz.sqldump.util.StringUtils;

public class DBObjectUtils {
	
	@Deprecated
	public static boolean equalsIgnoreWhitespacesEachLine(String s1, String s2) {
		return StringUtils.equalsIgnoreWhitespacesEachLine(s1, s2);
	}
	
}
