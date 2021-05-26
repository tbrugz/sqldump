package tbrugz.sqldump.sqlrun.tokenzr;

public class TokenizerUtil {

	public static boolean containsSqlStatmement(String sql) {
		return removeSqlComents(sql).trim().length()>0;
	}
	
	enum TknState {
		DEFAULT, STRING, COMM_LINE, COMM_BLOCK
	}
	
	public static String removeSqlComents(String sql) {
		TknState state = TknState.DEFAULT;
		StringBuilder ret = new StringBuilder();

		int len = sql.length();
		for(int i=0;i<len;i++) {
			char c = sql.charAt(i);
			boolean lastChar = (i==len-1);
			switch (state) {
				case DEFAULT: {
					if(c=='\'') {
						state = TknState.STRING;
						ret.append(c);
					}
					else if(c=='-' && !lastChar) {
						char c2 = sql.charAt(i+1);
						if(c2=='-') { state = TknState.COMM_LINE; }
					}
					else if(c=='/' && !lastChar) {
						char c2 = sql.charAt(i+1);
						if(c2=='*') { state = TknState.COMM_BLOCK; }
					}
					else {
						ret.append(c);
					}
				}
				case STRING: {
					if(c=='\'') {
						state = TknState.DEFAULT;
						ret.append(c);
					}
				}
				case COMM_LINE: {
					if(c=='\n') {
						state = TknState.DEFAULT;
						ret.append(c);
					}
				}
				case COMM_BLOCK: {
					if(c=='*' && !lastChar) {
						char c2 = sql.charAt(i+1);
						if(c2=='/') {
							state = TknState.DEFAULT;
							i++; // skips next '/' char
						}
					}
				}
			}
		}
	
		//String sret = ret.toString();
		//System.out.println("["+sret+"]");
		return ret.toString();
	}
}
