package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.List;

public class Grant implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public String table;
	public PrivilegeType privilege;
	public String grantee;
	public boolean withGrantOption;
	
	@Override
	public String toString() {
		return "["+table+";priv="+privilege+";to:"+grantee+(withGrantOption?";GO!":"")+"]";
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((grantee == null) ? 0 : grantee.hashCode());
		result = prime * result
				+ ((privilege == null) ? 0 : privilege.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		result = prime * result + (withGrantOption ? 1231 : 1237);
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
		Grant other = (Grant) obj;
		if (grantee == null) {
			if (other.grantee != null)
				return false;
		} else if (!grantee.equals(other.grantee))
			return false;
		if (privilege != other.privilege)
			return false;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		if (withGrantOption != other.withGrantOption)
			return false;
		return true;
	}

	public static boolean containsGrant(List<Grant> grants, Grant grant) {
		if(grants.size()==0) { return false; }
		for(Grant g: grants) {
			if(g.equals(grant)) {
				return true;
			}
		}
		return false;
	}
}
