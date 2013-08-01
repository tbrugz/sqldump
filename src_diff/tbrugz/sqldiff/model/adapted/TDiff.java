package tbrugz.sqldiff.model.adapted;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldump.dbmodel.Table;

@XmlAccessorType(XmlAccessType.FIELD)
public class TDiff {
	public ChangeType changeType;
	public String renameFromSchema;
	public String renameFromName;
	public Table table;
}