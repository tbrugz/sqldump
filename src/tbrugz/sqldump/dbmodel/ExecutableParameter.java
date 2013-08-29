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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dataType == null) ? 0 : dataType.hashCode());
		result = prime * result + ((inout == null) ? 0 : inout.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + position;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ExecutableParameter other = (ExecutableParameter) obj;
		if (dataType == null) {
			if (other.dataType != null)
				return false;
		} else if (!dataType.equals(other.dataType))
			return false;
		if (inout != other.inout)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (position != other.position)
			return false;
		return true;
	}
	
}
