package tbrugz.sqldiff;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldiff.model.ColumnDiff;
import tbrugz.sqldiff.model.DBIdentifiableDiff;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.TableDiff;
import tbrugz.sqldiff.util.SimilarityCalculator;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.Index;
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
	
	@Override
	public String toString() {
		return "RenameTuple[ add="+add+" ; drop="+drop+" ]";
	}
}

public class RenameDetector {
	
	static final Log log = LogFactory.getLog(RenameDetector.class);
	
	static List<RenameTuple> detectTableRenames(Collection<TableDiff> tableDiffs, double minSimilarity) {
		List<TableDiff> ltadd = getDiffsOfType(tableDiffs, ChangeType.ADD);
		List<TableDiff> ltdrop = getDiffsOfType(tableDiffs, ChangeType.DROP);
		
		log.debug("ltadd-size: '"+ltadd.size()+"' ; ltdrop-size: '"+ltdrop.size()+"'");
		if(ltadd.size()==0 || ltdrop.size()==0) {
			return new ArrayList<RenameTuple>();
		}
		
		//if multiple renames, return ok if all that sim > minSim
		List<RenameTuple> renames = new ArrayList<RenameTuple>();
		for(TableDiff tadd: ltadd) {
			for(TableDiff tdrop: ltdrop) {
				double similarity = SimilarityCalculator.instance().similarity(tadd.getTable(), tdrop.getTable());
				log.debug("t-add: '"+tadd.getNamedObject()+"' ; t-drop: '"+tdrop.getNamedObject()+"' ; sim: "+similarity);
				if(similarity>=minSimilarity) {
					renames.add(new RenameTuple(tadd, tdrop, similarity));
				}
			}
		}
		
		return renames;
	}
	
	static List<RenameTuple> detectColumnRenames(Collection<ColumnDiff> columnDiffs, double minSimilarity) {
		List<ColumnDiff> lcadd = getDiffsOfType(columnDiffs, ChangeType.ADD);
		List<ColumnDiff> lcdrop = getDiffsOfType(columnDiffs, ChangeType.DROP);
		
		log.debug("lcadd-size: '"+lcadd.size()+"' ; lcdrop-size: '"+lcdrop.size()+"'");
		if(lcadd.size()==0 || lcdrop.size()==0) {
			return new ArrayList<RenameTuple>();
		}
		
		Set<String> colsAdded = new HashSet<String>();
		Set<String> colsDropped = new HashSet<String>();
		List<RenameTuple> renames = new ArrayList<RenameTuple>();
		
		for(int i=0;i<lcadd.size();i++) {
			ColumnDiff cadd = lcadd.get(i);
			for(int j=0;j<lcdrop.size();j++) {
				ColumnDiff cdrop = lcdrop.get(j);
				if(!cadd.getNamedObject().equals(cdrop.getNamedObject())) {
					//log.debug("different tables... c-add: '"+cadd+"' ; c-drop: '"+cdrop+"'");
					continue;
				}
				double similarity = SimilarityCalculator.instance().similarity(cadd.getColumn(), cdrop.getPreviousColumn());
				log.debug("same tables... c-add: '"+cadd+"' ["+cadd.getColumn().getOrdinalPosition()+"] ; c-drop: '"+cdrop+"' ["+cdrop.getPreviousColumn().getOrdinalPosition()+"]; sim: "+similarity);
				if(similarity>=minSimilarity) {
					log.debug("renamed; c-add: '"+cadd+"' ; c-drop: '"+cdrop+"' ; sim: "+similarity);
					String cAddId = cadd.getNamedObject().getSchemaName()+"."+cadd.getNamedObject().getName()+"."+cadd.getColumn().getName();
					String cDropId = cdrop.getNamedObject().getSchemaName()+"."+cdrop.getNamedObject().getName()+"."+cdrop.getPreviousColumn().getName();
					if(!colsDropped.add(cDropId)) {
						log.warn("column '"+cDropId+"' \"renamed-from\" multiple times");
					}
					if(!colsAdded.add(cAddId)) {
						log.warn("column '"+cAddId+"' \"renamed-to\" multiple times");
					}
					renames.add(new RenameTuple(cadd, cdrop, similarity));
				}
			}
		}
		
		return renames;
	}

	static List<RenameTuple> detectIndexRenames(Collection<DBIdentifiableDiff> dbidDiffs, double minSimilarity) {
		List<DBIdentifiableDiff> lIdAdd = getDiffsOfType(dbidDiffs, ChangeType.ADD);
		List<DBIdentifiableDiff> lIdDrop = getDiffsOfType(dbidDiffs, ChangeType.DROP);
		
		log.debug("lIdAdd-size: '"+lIdAdd.size()+"' ; lIdDrop-size: '"+lIdDrop.size()+"'");
		if(lIdAdd.size()==0 || lIdDrop.size()==0) {
			return new ArrayList<RenameTuple>();
		}
		
		List<RenameTuple> renames = new ArrayList<RenameTuple>();
		
		for(int i=0;i<lIdAdd.size();i++) {
			DBIdentifiableDiff diffAdd = lIdAdd.get(i);
			if(diffAdd.getObjectType()!=DBObjectType.INDEX) { continue; }
			for(int j=0;j<lIdDrop.size();j++) {
				DBIdentifiableDiff diffDrop = lIdDrop.get(j);
				if(diffAdd.getObjectType()!=DBObjectType.INDEX) { continue; }
				
				//XXX: SimilarityCalculator...
				double similarity = 1;
				if(equalsApartFromName(diffAdd.ident(), diffDrop.ident())) {
					renames.add(new RenameTuple(diffAdd, diffDrop, similarity));
				}
			}
		}
		
		return renames;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	static void doRenames(Collection/*<? extends Diff>*/ diffs, List<RenameTuple> renames) {
		for(RenameTuple rt: renames) {
			boolean added = false;
			int removedCount = 0;
			if(rt.add instanceof TableDiff) {
				TableDiff tdrename = new TableDiff(ChangeType.RENAME, (Table) rt.add.getNamedObject(), rt.drop.getNamedObject().getSchemaName(), rt.drop.getNamedObject().getName());
				added = diffs.add(tdrename);
				//TODO add & drop columns!
			}
			else if(rt.add instanceof ColumnDiff) {
				ColumnDiff cdrename = new ColumnDiff(ChangeType.RENAME, rt.add.getNamedObject(), ((ColumnDiff)rt.drop).getPreviousColumn(), ((ColumnDiff)rt.add).getColumn());
				added = diffs.add(cdrename);
			}
			else if(rt.add instanceof DBIdentifiableDiff) {
				DBIdentifiableDiff dff = (DBIdentifiableDiff) rt.add;
				DBIdentifiableDiff idrename = new DBIdentifiableDiff(ChangeType.RENAME, (DBIdentifiable) rt.drop.getNamedObject(), (DBIdentifiable) rt.add.getNamedObject(), dff.getOwnerTableName());
				added = diffs.add(idrename);
			}
			if(!added) { throw new RuntimeException("could not add rename: "+rt); }
			removedCount += diffs.remove(rt.add)?1:0;
			removedCount += diffs.remove(rt.drop)?1:0;
			if(removedCount!=2) { throw new RuntimeException("could not remove add/drop diffs: "+rt); }
		}
	}

	static void validateConflictingRenames(List<RenameTuple> renames) {
		Set<Diff> stadd = new HashSet<Diff>();
		Set<Diff> stdrop = new HashSet<Diff>();
		for(RenameTuple rt: renames) {
			//throw UndecidableRenameException ?
			if(! stadd.add(rt.add)) {
				throw new ConflictingChangesException("a table can't be renamed more than once [add table = '"+rt.add+"'; similarity = "+rt.similarity+"]");
			}
			if(! stdrop.add(rt.drop)) {
				throw new ConflictingChangesException("a table can't be renamed more than once [drop table = '"+rt.drop+"'; similarity = "+rt.similarity+"]");
			}
		}
	}
	
	public static int detectAndDoTableRenames(Collection<TableDiff> tableDiffs, double minSimilarity) {
		//get renames
		List<RenameTuple> renames = detectTableRenames(tableDiffs, minSimilarity);
		
		//validate & do renames
		return validateAndDoRenames(tableDiffs, renames, "table");
	}

	public static int detectAndDoColumnRenames(Collection<ColumnDiff> columnDiffs, double minSimilarity) {
		//get renames
		List<RenameTuple> renames = detectColumnRenames(columnDiffs, minSimilarity);

		//validate & do renames
		return validateAndDoRenames(columnDiffs, renames, "column");
	}

	public static int detectAndDoIndexRenames(Collection<DBIdentifiableDiff> dbIdDiffs, double minSimilarity) {
		//get renames
		List<RenameTuple> renames = detectIndexRenames(dbIdDiffs, minSimilarity);

		//validate & do renames
		return validateAndDoRenames(dbIdDiffs, renames, "index");
	}

	static int validateAndDoRenames(Collection<? extends Diff> diffs, List<RenameTuple> renames, String diffType) {
		if(renames.size()==0) {
			log.info("no "+diffType+" renames detected");
		}
		else {
			log.info(renames.size()+" "+diffType+" renames detected");

			//validate renames
			validateConflictingRenames(renames);
			
			//do renames
			doRenames(diffs, renames);
		}

		return renames.size();
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
	
	static boolean equalsApartFromName(DBIdentifiable o1, DBIdentifiable o2) {
		if(o1 instanceof Index && o2 instanceof Index) {
			Index i1 = (Index) o1;
			Index i2 = (Index) o2;
			if(i1.getColumns().equals(i2.getColumns())
				&& i1.getLocal()==i2.getLocal()
				&& i1.getReverse()==i2.getReverse()
				&& i1.getIndexType()==i2.getIndexType()
				&& i1.getTableName().equals(i2.getTableName())
				&& ( i1.getType()==null && i2.getType()==null || i1.getType().equals(i2.getType()) )
				&& i1.isUnique() == i2.isUnique()
				) {
				return true;
			}
		}
		
		return false;
	}
}
