package tbrugz.sqldiff.model;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import tbrugz.sqldiff.model.adapted.TCDiff;

public class TableColumnDiffAdapter extends XmlAdapter<TCDiff, TableColumnDiff> {
	
	@Override
	public TableColumnDiff unmarshal(TCDiff v) throws Exception {
		return new TableColumnDiff(v.changeType, v.table, v.previousColumn, v.column);
	}

	@Override
	public TCDiff marshal(TableColumnDiff v) throws Exception {
		TCDiff adapted = new TCDiff();
		adapted.changeType = v.getChangeType();
		adapted.table = v.table;
		adapted.column = v.column;
		adapted.previousColumn = v.previousColumn;
		return adapted;
	}

}
