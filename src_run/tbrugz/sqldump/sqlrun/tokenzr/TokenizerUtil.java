package tbrugz.sqldump.sqlrun.tokenzr;

import java.util.ArrayList;
import java.util.List;

public class TokenizerUtil {

	public static class QueryParameter {
		public final String name;
		public final int position;

		public QueryParameter(String name, int position) {
			this.name = name;
			this.position = position;
		}

		public String toString() {
			return "QueryParameter["+name+";"+position+"]";
		}
	}

	public static boolean containsSqlStatmement(String sql) {
		if(sql==null) { return false; }
		return removeSqlComents(sql).trim().length()>0;
	}
	
	enum TknState {
		DEFAULT, STRING, DQUOTE, /*NAMEDPAR,*/ COMM_LINE, COMM_BLOCK
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
					else if(c=='"') {
						state = TknState.DQUOTE;
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
					break;
				}
				case STRING: {
					if(c=='\'') {
						state = TknState.DEFAULT;
						ret.append(c);
					}
					else {
						ret.append(c);
					}
					break;
				}
				case DQUOTE: {
					if(c=='"') {
						state = TknState.DEFAULT;
						ret.append(c);
					}
					else {
						ret.append(c);
					}
					break;
				}
				case COMM_LINE: {
					if(c=='\n') {
						state = TknState.DEFAULT;
						ret.append(c);
					}
					break;
				}
				case COMM_BLOCK: {
					if(c=='*' && !lastChar) {
						char c2 = sql.charAt(i+1);
						if(c2=='/') {
							state = TknState.DEFAULT;
							i++; // skips next '/' char
						}
					}
					break;
				}
			}
		}
	
		//String sret = ret.toString();
		//System.out.println("["+sret+"]");
		return ret.toString();
	}

	/**
	 * Return named parameters - defined as <code>:[a-zA-Z_][0-9a-zA-Z_]*</code>. Ex: <code>:someParam</code>
	 */
	public static List<QueryParameter> getNamedParameters(String sql) {
		if(sql==null) { return null; }
		TknState state = TknState.DEFAULT;
		List<QueryParameter> ret = new ArrayList<QueryParameter>();
		int countPositionalParameters = 0;

		int len = sql.length();
		for(int i=0;i<len;i++) {
			char c = sql.charAt(i);
			boolean lastChar = (i==len-1);
			switch (state) {
				case DEFAULT: {
					if(c=='\'') {
						state = TknState.STRING;
					}
					else if(c=='"') {
						state = TknState.DQUOTE;
					}
					else if(c=='-' && !lastChar) {
						char c2 = sql.charAt(i+1);
						if(c2=='-') { state = TknState.COMM_LINE; }
					}
					else if(c=='/' && !lastChar) {
						char c2 = sql.charAt(i+1);
						if(c2=='*') { state = TknState.COMM_BLOCK; }
					}
					else if(c==':' && !lastChar) {
						StringBuilder sb = new StringBuilder();
						int pos = i;
						char cp = sql.charAt(++i);
						if( (cp>='A' && cp<='Z') || (cp>='a' && cp<='z') || cp == '_' ) {
							while( (cp>='A' && cp<='Z') || (cp>='a' && cp<='z') || (cp>='0' && cp<='9') || cp == '_') {
								sb.append(cp);
								if(i==len-1) { break; }
								cp = sql.charAt(++i);
							}
						}
						String pName = sb.toString();
						if(pName.length()>0) {
							QueryParameter qp = new QueryParameter(pName, pos);
							ret.add(qp);
							//System.out.println("named: "+qp);
						}
						else {
							// ignore empty parameter
						}
					}
					else if(c=='?') {
						// positional param?
						countPositionalParameters++;
						//System.out.println("positional: "+i);
					}
					break;
				}
				case STRING: {
					if(c=='\'') {
						state = TknState.DEFAULT;
					}
					break;
				}
				case DQUOTE: {
					if(c=='"') {
						state = TknState.DEFAULT;
					}
					break;
				}
				case COMM_LINE: {
					if(c=='\n') {
						state = TknState.DEFAULT;
					}
					break;
				}
				case COMM_BLOCK: {
					if(c=='*' && !lastChar) {
						char c2 = sql.charAt(i+1);
						if(c2=='/') {
							state = TknState.DEFAULT;
							i++; // skips next '/' char
						}
					}
					break;
				}
			}
		}

		if(countPositionalParameters>0 && ret.size()>0) {
			//System.out.println(">> sql:\n"+sql);
			throw new IllegalStateException(
				"can't have both positional [#"+countPositionalParameters+"] "+
				"and named [#"+ret.size()+"] parameters in the same query"
				);
		}
		return ret;
	}

	public static String replaceNamedParameters(String sql, List<QueryParameter> pars) {
		if(sql==null) { return null; }
		StringBuilder sb = new StringBuilder();
		sb.append(sql);
		for(QueryParameter qp: pars) {
			int plen = qp.name.length();
			String spaces = new String(new char[plen]).replace("\0", " ");
			String replacement = "?" + spaces;
			sb.replace(qp.position, qp.position+plen+1, replacement);
		}
		return sb.toString();
	}

	public static String replaceNamedParameters(String sql) {
		List<QueryParameter> qpl = TokenizerUtil.getNamedParameters(sql);
		if(qpl.size()>0) {
			return TokenizerUtil.replaceNamedParameters(sql, qpl);
		}
		return sql;
	}

	public static List<String> getParameterNames(List<QueryParameter> pars) {
		List<String> ret = new ArrayList<String>();
		for(QueryParameter qp: pars) {
			ret.add(qp.name);
		}
		return ret;
	}
	
}
