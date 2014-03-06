package tbrugz.sqldiff.model.adapted;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldump.dbmodel.PrivilegeType;

@XmlAccessorType(XmlAccessType.FIELD)
public class GrntDiff {
	public ChangeType changeType;
	public String schemaName;
	public String tableName;
	public PrivilegeType privilege;
	public String grantee;
	public boolean withGrantOption;
}
