package tbrugz.sqldiff.model.adapted;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;

@XmlAccessorType(XmlAccessType.FIELD)
public class TCDiff {
	public ChangeType changeType;
	public Table table;
	public Column column;
	public Column previousColumn;
}