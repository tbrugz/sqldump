package tbrugz.sqldump.graph;

import tbrugz.graphml.model.Node;

public class TableNode extends Node {
	String columnsDesc;
	String constraintsDesc; // = "";
	String stereotype;
	int columnNumber = 0;
	boolean root = false;
	boolean leaf = false;

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

	public String getConstraintsDesc() {
		return constraintsDesc;
	}

	public void setConstraintsDesc(String constraintsDesc) {
		this.constraintsDesc = constraintsDesc;
	}

	public boolean isRoot() {
		return root;
	}

	public void setRoot(boolean root) {
		this.root = root;
	}

	public boolean isLeaf() {
		return leaf;
	}

	public void setLeaf(boolean leaf) {
		this.leaf = leaf;
	}

}
