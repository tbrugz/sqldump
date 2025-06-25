package tbrugz.sqldump.resultset.pivot;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

public class KeyTest {

	static final Log log = LogFactory.getLog(KeyTest.class);

	@Test
	public void compareTo() {
		Key k1 = new Key(new String[]{"1","2","3"});
		Key k2 = new Key(new String[]{"1","2","3","4"});
		Key k3 = new Key(new String[]{"1","2","4"});
		
		Assert.assertEquals(0, k1.compareTo(k1));
		Assert.assertEquals(-1, k1.compareTo(k2));
		Assert.assertEquals(1, k2.compareTo(k1));
		Assert.assertEquals(-1, k1.compareTo(k3));
		Assert.assertEquals(1, k3.compareTo(k1));
		Assert.assertEquals(-1, k2.compareTo(k3));
		Assert.assertEquals(1, k3.compareTo(k2));
	}

	@Test
	public void compareToWithNulls() {
		Key k1 = new Key(new String[]{"1","2","3"});
		Key k2 = new Key(new String[]{"1","2",null,"4"});
		Key k3 = new Key(new String[]{"1","2",null,"5"});
		
		Assert.assertEquals(0, k1.compareTo(k1));
		Assert.assertEquals(-1, k1.compareTo(k2));
		Assert.assertEquals(1, k2.compareTo(k1));
		Assert.assertEquals(-1, k1.compareTo(k3));
		Assert.assertEquals(1, k3.compareTo(k1));
		Assert.assertEquals(-1, k2.compareTo(k3));
		Assert.assertEquals(1, k3.compareTo(k2));
	}
	
	@Test
	public void copy() {
		Key k1 = new Key(new String[]{"0","1","2","3","4"});
		Key k2 = k1.copy();
		Key k3 = k1.copy(2);
		Key k4 = k1.copy(2, 4);
		
		//log.debug(k3);
		//log.debug(k4);
		
		Assert.assertEquals(k1, k2);
		Assert.assertArrayEquals(Arrays.copyOfRange(k1.values, 2, k1.values.length), k3.values);
		Assert.assertArrayEquals(Arrays.copyOfRange(k1.values, 2, k1.values.length-1), k4.values);
	}

}
