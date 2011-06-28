package tbrugz.sqldump.graph;

import tbrugz.graphml.model.Node;

public class TableNode extends Node {
	String columnsDesc;
	String stereotype;

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
}
