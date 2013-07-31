package tbrugz.sqldiff.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldiff.model.ColumnDiff;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.TableDiff;
import tbrugz.sqldump.dbmodel.Table;

class RenameTuple {
	Diff add;
	Diff drop;
	double similarity;
	
	public RenameTuple(Diff add, Diff drop, double similarity) {
		this.add = add;
		this.drop = drop;
		this.similarity = similarity;
	}
}

public class RenameDetector {
	
	static final Log log = LogFactory.getLog(RenameDetector.class);
	
	public static void detectTableRenames(Collection<TableDiff> tableDiffs, double minSimilarity) {
		/*
		List<TableDiff> ltadd = new ArrayList<TableDiff>();
		List<TableDiff> ltdrop = new ArrayList<TableDiff>();
		for(TableDiff td: tableDiffs) {
			switch (td.getChangeType()) {
			case ADD:
				ltadd.add(td);
				break;
			case DROP:
				ltdrop.add(td);
				break;
			default:
				break;
			}
		}
		*/
		List<TableDiff> ltadd = getDiffsOfType(tableDiffs, ChangeType.ADD);
		List<TableDiff> ltdrop = getDiffsOfType(tableDiffs, ChangeType.DROP);
		
		log.info("ltadd-size: '"+ltadd.size()+"' ; ltdrop-size: '"+ltdrop.size());
		if(ltadd.size()==0 || ltdrop.size()==0) {
			return;
		}
		
		//if multiple renames, return ok if all that sim > minSim
		List<RenameTuple> renames = new ArrayList<RenameTuple>();
		Set<TableDiff> stadd = new HashSet<TableDiff>();
		Set<TableDiff> stdrop = new HashSet<TableDiff>();
		for(TableDiff tadd: ltadd) {
			for(TableDiff tdrop: ltdrop) {
				double similarity = SimilarityCalculator.instance().similarity(tadd.getTable(), tdrop.getTable());
				log.info("t-add: '"+tadd.getNamedObject()+"' ; t-drop: '"+tdrop.getNamedObject()+"' ; sim: "+similarity);
				if(similarity>=minSimilarity) {
					renames.add(new RenameTuple(tadd, tdrop, similarity));
					//throw UndecidableRenameException ?
					if(! stadd.add(tadd)) {
						throw new RuntimeException("can't add a table to more than one RenameTuple [table = '"+tdrop+"'; minSimilarity = "+minSimilarity+"]");
					}
					if(! stdrop.add(tdrop)) {
						throw new RuntimeException("can't add a table to more than one RenameTuple [table = '"+tdrop+"'; minSimilarity = "+minSimilarity+"]");
					}
				}
			}
		}
		
		//do renames
		doRenames(tableDiffs, renames);
	}

	//TODO detectColumnRenames
	public static void detectColumnRenames(Collection<ColumnDiff> tableDiffs, double minSimilarity) {
	}
	
	static void doRenames(Collection/*<? extends Diff>*/ diffs, List<RenameTuple> renames) {
		for(RenameTuple rt: renames) {
			boolean added = false;
			int removedCount = 0;
			if(rt.add instanceof TableDiff) {
				TableDiff tdrename = new TableDiff(ChangeType.RENAME, (Table) rt.add.getNamedObject(), rt.drop.getNamedObject().getName());
				added = diffs.add(tdrename);
				//TODO add & drop columns!
			}
			else if(rt.add instanceof ColumnDiff) {
				//TODO: column renames
			}
			if(!added) { throw new RuntimeException("could not add rename: "+rt); }
			removedCount += diffs.remove(rt.add)?1:0;
			removedCount += diffs.remove(rt.drop)?1:0;
			if(removedCount!=2) { throw new RuntimeException("could not remove add/drop diffs: "+rt); }
		}
	}
	
	static <T extends Diff> List<T> getDiffsOfType(Collection<T> difflist, ChangeType type) {
		List<T> ret = new ArrayList<T>();
		for(T d: difflist) {
			if(d.getChangeType().equals(type)) {
				ret.add(d);
			}
		}
		return ret;
	}
}
