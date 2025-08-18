package tbrugz.sqldiff.model.adapted;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;

import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldump.dbmodel.Table;

@XmlAccessorType(XmlAccessType.FIELD)
public class TDiff {
	public ChangeType changeType;
	public String renameFromSchema;
	public String renameFromName;
	public String previousRemarks;
	public Table table;
}
