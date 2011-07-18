package tbrugz.sqldump.dbmodel;

import java.io.Serializable;

public class Grant implements Serializable {
	public String table;
	public PrivilegeType privilege;
	public String grantee;
	public boolean withGrantOption;
}
