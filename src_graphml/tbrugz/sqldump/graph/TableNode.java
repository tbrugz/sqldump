package tbrugz.sqldump.graph;

import tbrugz.graphml.model.Node;

public class TableNode extends Node {
	String columnsDesc;

	public String getColumnsDesc() {
		return columnsDesc;
	}

	public void setColumnsDesc(String columnsDesc) {
		this.columnsDesc = columnsDesc;
	}
}
