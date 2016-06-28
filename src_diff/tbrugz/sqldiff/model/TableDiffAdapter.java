package tbrugz.sqldiff.model;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import tbrugz.sqldiff.model.adapted.TDiff;

public class TableDiffAdapter extends XmlAdapter<TDiff, TableDiff> {
	
	@Override
	public TableDiff unmarshal(TDiff v) throws Exception {
		return new TableDiff(v.changeType, v.table, v.renameFromSchema, v.renameFromName, v.newRemarks);
	}

	@Override
	public TDiff marshal(TableDiff v) throws Exception {
		TDiff adapted = new TDiff();
		adapted.changeType = v.getChangeType();
		adapted.table = v.table;
		adapted.renameFromSchema = v.renameFromSchema;
		adapted.renameFromName = v.renameFromName;
		adapted.newRemarks = v.newRemarks;
		return adapted;
	}

}
