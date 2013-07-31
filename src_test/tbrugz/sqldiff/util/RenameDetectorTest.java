package tbrugz.sqldiff.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldiff.model.ColumnDiffTest;
import tbrugz.sqldiff.model.TableDiff;
import tbrugz.sqldump.dbmodel.Table;

public class RenameDetectorTest {

	Table t1 = new Table();
	Table t2 = new Table();
	
	@Before
	public void setup() {
		t1.setName("t");
		t1.getColumns().add(ColumnDiffTest.newColumn("c1", "varchar", 10));
		t1.getColumns().add(ColumnDiffTest.newColumn("c2", "varchar", 10));

		t2.setName("t");
		t2.getColumns().add(ColumnDiffTest.newColumn("c2", "varchar", 10));
		t2.getColumns().add(ColumnDiffTest.newColumn("c1", "varchar", 10));
	}

	@Test
	public void testDoRename1() {
		TableDiff td1 = new TableDiff(ChangeType.ADD, t1);
		TableDiff td2 = new TableDiff(ChangeType.DROP, t1);
		List<TableDiff> lt = new ArrayList<TableDiff>();
		lt.add(td1); lt.add(td2);
		
		RenameDetector.detectTableRenames(lt, 0.5);
		Assert.assertEquals(1, lt.size());
		Assert.assertEquals(ChangeType.RENAME, lt.get(0).getChangeType());
	}

	@Test
	public void testDoRename2() {
		TableDiff td1 = new TableDiff(ChangeType.ADD, t1);
		TableDiff td2 = new TableDiff(ChangeType.DROP, t2);
		List<TableDiff> lt = new ArrayList<TableDiff>();
		lt.add(td1); lt.add(td2);
		
		RenameDetector.detectTableRenames(lt, 0.5);
		Assert.assertEquals(1, lt.size());
		Assert.assertEquals(ChangeType.RENAME, lt.get(0).getChangeType());
	}

	@Test(expected=RuntimeException.class)
	public void testErrorRename1() {
		TableDiff td1 = new TableDiff(ChangeType.ADD, t1);
		TableDiff td2 = new TableDiff(ChangeType.DROP, t2);
		TableDiff td3 = new TableDiff(ChangeType.DROP, t1);
		List<TableDiff> lt = new ArrayList<TableDiff>();
		lt.add(td1); lt.add(td2); lt.add(td3);
		
		// which to rename? which to drop?
		RenameDetector.detectTableRenames(lt, 0.5); //0.9?
	}

	@Test(expected=RuntimeException.class)
	public void testErrorRename2() {
		TableDiff td1 = new TableDiff(ChangeType.ADD, t1);
		TableDiff td2 = new TableDiff(ChangeType.DROP, t2);
		TableDiff td3 = new TableDiff(ChangeType.DROP, t1);
		TableDiff td4 = new TableDiff(ChangeType.ADD, t2);
		List<TableDiff> lt = new ArrayList<TableDiff>();
		lt.add(td1); lt.add(td2); lt.add(td3); lt.add(td4);
		
		RenameDetector.detectTableRenames(lt, 0.5);
	}
}
