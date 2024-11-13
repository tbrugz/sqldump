package tbrugz.sqldiff;

public enum WhitespaceIgnoreType {
	
	NONE, EOL, SOL, SOL_EOL, ALL;
	
	public static WhitespaceIgnoreType getDefault() {
		return EOL;
	}
	
	public static WhitespaceIgnoreType getType(String strType) {
		if(strType==null || strType.equals("")) {
			return getDefault();
		}

		if(strType.equalsIgnoreCase("none")) {
			return NONE;
		}
		if(strType.equalsIgnoreCase("eol")) {
			return EOL;
		}
		if(strType.equalsIgnoreCase("sol")) {
			return SOL;
		}
		if(strType.equalsIgnoreCase("sol_eol") || strType.equalsIgnoreCase("sol+eol")) {
			return SOL_EOL;
		}
		if(strType.equalsIgnoreCase("all")) {
			return ALL;
		}
		throw new IllegalArgumentException("Unknown whitespace diff type: "+strType);
	}
	
	public boolean stripSol() {
		return this==SOL || this==SOL_EOL || this==ALL;
	}

	public boolean stripEol() {
		return this==EOL || this==SOL_EOL || this==ALL;
	}
	
	public boolean stripInside() {
		return this==ALL;
	}
	
}
