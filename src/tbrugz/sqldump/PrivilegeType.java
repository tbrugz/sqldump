package tbrugz.sqldump;

public enum PrivilegeType {
	SELECT, INSERT, UPDATE, DELETE, ALTER, REFERENCES, INDEX;
	//DEBUG, FLASHBACK, ON COMMIT REFRESH, QUERY REWRITE
	
	//public PrivilegeType valueOf(String s) { return null; }
}
