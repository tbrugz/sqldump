package tbrugz.sqldump.dbmodel;

import java.io.Serializable;

public class ExecutableParameter implements Serializable {
	private static final long serialVersionUID = 1L;

	public enum INOUT {
		IN,
		OUT,
		INOUT;
		
		public static INOUT getValue(String s) {
			if("IN/OUT".equals(s)) { return INOUT; }
			return valueOf(s);
		}
	}
	
	public String name;
	public int position;
	public String dataType;
	public INOUT inout;
	
	@Override
	public String toString() {
		return "EP["+name+";"+position+";"+dataType+";"+inout+"]";
	}
}
