package tbrugz.sqldiff.model;

import jakarta.xml.bind.annotation.adapters.XmlAdapter;

import tbrugz.sqldiff.model.adapted.DBIdDiff;

public class DBIdentifiableDiffAdapter extends XmlAdapter<DBIdDiff, DBIdentifiableDiff> {
	
	@Override
	public DBIdentifiableDiff unmarshal(DBIdDiff v) throws Exception {
		return new DBIdentifiableDiff(v.changeType, v.previousIdent, v.ident, v.ownerTableName);
	}

	@Override
	public DBIdDiff marshal(DBIdentifiableDiff v) throws Exception {
		DBIdDiff adapted = new DBIdDiff();
		adapted.changeType = v.getChangeType();
		adapted.ident = v.ident;
		adapted.ownerTableName = v.ownerTableName;
		adapted.previousIdent = v.previousIdent;
		return adapted;
	}

}
