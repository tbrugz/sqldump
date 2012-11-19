package tbrugz.sqldump.dbmodel;

import java.io.Serializable;

public class ExecutableParameter implements Serializable {
	public enum INOUT {
		IN,
		OUT,
		INOUT
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
