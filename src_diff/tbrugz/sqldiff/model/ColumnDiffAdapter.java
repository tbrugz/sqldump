package tbrugz.sqldiff.model;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import tbrugz.sqldiff.model.adapted.TCDiff;

public class ColumnDiffAdapter extends XmlAdapter<TCDiff, ColumnDiff> {
	
	@Override
	public ColumnDiff unmarshal(TCDiff v) throws Exception {
		return new ColumnDiff(v.changeType, v.schemaName, v.tableName, v.previousColumn, v.column);
	}

	@Override
	public TCDiff marshal(ColumnDiff v) throws Exception {
		TCDiff adapted = new TCDiff();
		adapted.changeType = v.getChangeType();
		adapted.schemaName = v.schemaName;
		adapted.tableName = v.tableName;
		adapted.column = v.column;
		adapted.previousColumn = v.previousColumn;
		return adapted;
	}

}
