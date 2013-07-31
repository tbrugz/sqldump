package tbrugz.sqldiff.util;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;

public class SimilarityCalculator {
	
	private SimilarityCalculator() {}
	
	static SimilarityCalculator instance = new SimilarityCalculator();
	
	public static SimilarityCalculator instance() {
		return instance;
	}

	public double similarity(Table t1, Table t2) {
		final double NAME_WEIGTH = 0.1; 
		final double SCHEMANAME_WEIGTH = 0.05;
		final double COLUMNS_WEIGTH = 0.7;
		
		final double ALL_WEIGTH = NAME_WEIGTH+SCHEMANAME_WEIGTH+COLUMNS_WEIGTH;
		
		double ret = 0;
		
		//name similarity
		if(t1.getSchemaName()==null && t2.getSchemaName()==null) {
			ret += SCHEMANAME_WEIGTH;
		}
		else if(t1.getSchemaName()!=null && t1.getSchemaName().equals(t2.getSchemaName())) {
			ret += SCHEMANAME_WEIGTH;
		}
		if(t1.getName().equals(t2.getName())) {
			ret += NAME_WEIGTH;
		}
		//columns similarity
		int t1cols = t1.getColumns().size();
		int t2cols = t1.getColumns().size();
		int sumSize = t1cols+t2cols;
		for(int i=0;i<t1cols;i++) {
			Column c1 = t1.getColumns().get(i);
			for(int j=0;j<t1cols;j++) {
				Column c2 = t2.getColumns().get(j);
				double ORDER_WEIGTH = 0.7;
				if(i==j) {
					//same column order!
					ORDER_WEIGTH = 1;
				}
				
				if(c1.equals(c2)) {
					ret += 2*COLUMNS_WEIGTH*ORDER_WEIGTH/sumSize;
				}
				else if(c1.getName().equals(c2.getName())) {
					ret += COLUMNS_WEIGTH*ORDER_WEIGTH/sumSize;
				}
			}
		}
		//XXX constraints similarity?
		
		return ret/ALL_WEIGTH;
	}

	//TODO Column similarity
	public double similarity(Column c1, Column c2) {
		return 0;
	}
}
