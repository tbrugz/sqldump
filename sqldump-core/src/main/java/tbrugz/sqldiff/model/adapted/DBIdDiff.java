package tbrugz.sqldiff.model.adapted;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;

import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldump.dbmodel.DBIdentifiable;

@XmlAccessorType(XmlAccessType.FIELD)
public class DBIdDiff {
	public ChangeType changeType;
	public DBIdentifiable ident;
	public DBIdentifiable previousIdent;
	public String ownerTableName;
}
