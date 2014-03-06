package tbrugz.sqldiff.model.adapted;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldump.dbmodel.Column;

@XmlAccessorType(XmlAccessType.FIELD)
public class TCDiff {
	public ChangeType changeType;
	public String schemaName;
	public String tableName;
	public Column column;
	public Column previousColumn;
}
