package tbrugz.sqldump.datadump;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

public class FFCTest {
	
	@Test
	public void statefulTest() throws CloneNotSupportedException {
		FFCDataDump ffc = new FFCDataDump();
		ffc.firstColSep = " x ";
		ffc.lsColNames.clear();
		ffc.colsMaxLenght = new ArrayList<Integer>();
		ffc.lsColNames.add("abc");
		ffc.colsMaxLenght.add(1);
		ffc.setColMaxLenghtForColNames();
		
		FFCDataDump ffcCloned = (FFCDataDump) ffc.clone();
		Assert.assertEquals(ffc.firstColSep, ffcCloned.firstColSep);
		//System.out.println("ffc: "+ffc.colsMaxLenght);
		//System.out.println("ffcCloned: "+ffcCloned.colsMaxLenght);
	}
	
	@Test
	public void replaceTabsFixed() throws CloneNotSupportedException {
		FFCDataDump ffc = new FFCDataDump();
		ffc.spacesForEachTab = 4;
		ffc.alignedTabReplacing = false;
		String original = "  		aa";
		Assert.assertEquals("          aa", ffc.replaceTabs(original));
	}
	
	@Test
	public void replaceTabsVariable() throws CloneNotSupportedException {
		FFCDataDump ffc = new FFCDataDump();
		ffc.spacesForEachTab = 4;
		ffc.alignedTabReplacing = true;
		String original = "  		aa";
		Assert.assertEquals("        aa", ffc.replaceTabs(original));
	}

	@Test
	public void replaceTabsVariableLength8() throws CloneNotSupportedException {
		FFCDataDump ffc = new FFCDataDump();
		ffc.spacesForEachTab = 8;
		ffc.alignedTabReplacing = true;
		String original = "  		aa";
		Assert.assertEquals("                aa", ffc.replaceTabs(original));
	}
	
}
