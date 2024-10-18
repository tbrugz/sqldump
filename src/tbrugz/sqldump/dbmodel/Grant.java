package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//XXX: make Grant immutable?
public class Grant implements DBType, Serializable {
	
	private static final long serialVersionUID = 1L;
	static final Log log = LogFactory.getLog(Grant.class);
	
	//XXX exclude 'table'? Grant is not (or should be) an 'independent' db object
	String table; //XXX rename to object? may be used by Views or Executables
	List<String> columns;
	PrivilegeType privilege;
	String grantee;
	boolean withGrantOption;
	
	@Deprecated
	public Grant(String owner, String column, PrivilegeType privilege, String grantee, boolean grantOption) {
		this(owner, (column!=null ? Arrays.asList( column ) : null), privilege, grantee, grantOption);
	}
	
	public Grant(String owner, List<String> columns, PrivilegeType privilege, String grantee, boolean grantOption) {
		this.table = owner;
		this.columns = columns;
		this.privilege = privilege;
		this.grantee = grantee;
		this.withGrantOption = grantOption;
	}

	public Grant(String owner, PrivilegeType privilege, String grantee) {
		this(owner, (List<String>) null, privilege, grantee, false);
	}
	
	public Grant() {
	}
	
	@Override
	public String toString() {
		return "["+table+";priv="+privilege+";to="+grantee
				+";"+(columns!=null?"cols="+columns:"")
				+";"+(withGrantOption?"GO!":"")+"]";
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

		if (columns == null) {
			if (other.columns != null)
				return false;
		} else if (!columns.equals(other.columns))
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

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public PrivilegeType getPrivilege() {
		return privilege;
	}

	public void setPrivilege(PrivilegeType privilege) {
		this.privilege = privilege;
	}

	public String getGrantee() {
		return grantee;
	}

	public void setGrantee(String grantee) {
		this.grantee = grantee;
	}

	public boolean isWithGrantOption() {
		return withGrantOption;
	}

	public void setWithGrantOption(boolean withGrantOption) {
		this.withGrantOption = withGrantOption;
	}
	
	public List<String> getColumns() {
		return columns;
	}
	
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	@Deprecated
	public String getColumn() {
		return (columns!=null && columns.size()>0)?columns.get(0):null;
	}

	public void setColumn(String column) {
		this.columns = column!=null ? Arrays.asList( column ) : null;
	}

	public static Grant parseGrant(final String grantStrPar) {
		try {
			String grantStr = grantStrPar.substring(1, grantStrPar.length()-1);
			String[] parts = grantStr.split(";", -1);
			//log.info("parts = "+Arrays.asList(parts));
			PrivilegeType privilege = PrivilegeType.valueOf(parts[1].substring(5));
			String grantee = parts[2].substring(3, parts[2].length());
			//String columnPart = parts[3];
			String columnsStr = (parts[3].length()>0)?parts[3].substring(6, parts[3].length()-1):null;
			List<String> columns = columnsStr!=null ? Arrays.asList( columnsStr.split(",") ) : null;
			//String column = (parts.length>=4)?parts[3].substring(4):null;
			boolean grantOption = (parts[4].length()>0)?parts[4].equals("GO!"):false;
			Grant g = new Grant(parts[0], columns, privilege, grantee, grantOption);
			return g;
		}
		catch(Exception e) {
			log.warn("parseGrant error ["+grantStrPar+"]");
			//log.info("parseGrant error ["+grantStrPar+"]", e);
			return null;
		}
	}

}
