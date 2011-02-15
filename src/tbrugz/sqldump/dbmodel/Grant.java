package tbrugz.sqldump.dbmodel;

public class Grant {
	public String table;
	public PrivilegeType privilege;
	public String grantee;
	public boolean withGrantOption;
}
