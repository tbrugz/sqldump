package tbrugz.sqldiff.model;

import java.util.List;

import tbrugz.sqldiff.util.DiffUtil;

public abstract class SingleDiff implements Diff {

	@Override
	public List<String> getDiffList() {
		return DiffUtil.singleElemList( getDiff() );
	}

	@Override
	public int getDiffListSize() {
		return 1;
	}

}
