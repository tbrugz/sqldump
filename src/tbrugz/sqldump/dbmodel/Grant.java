package tbrugz.sqldump.dbmodel;

import java.io.Serializable;

public class Grant implements Serializable {
	private static final long serialVersionUID = 1L;
	public String table;
	public PrivilegeType privilege;
	public String grantee;
	public boolean withGrantOption;
}
