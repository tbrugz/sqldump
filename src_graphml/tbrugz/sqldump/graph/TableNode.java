package tbrugz.sqldump.graph;

import tbrugz.graphml.model.Node;

public class TableNode extends Node {
	String columnsDesc;
	String stereotype;
	int columnNumber = 0;

	public String getColumnsDesc() {
		return columnsDesc;
	}

	public void setColumnsDesc(String columnsDesc) {
		this.columnsDesc = columnsDesc;
	}
	
	@Override
	public String getStereotype() {
		return stereotype;
	}
	
	@Override
	public void setStereotype(String s) {
		this.stereotype = s;
	}

	public int getColumnNumber() {
		return columnNumber;
	}

	public void setColumnNumber(int columnNumber) {
		this.columnNumber = columnNumber;
	}
	
}
