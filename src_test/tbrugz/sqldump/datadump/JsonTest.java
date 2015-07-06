package tbrugz.sqldump.datadump;

import java.text.DateFormat;

import org.junit.Assert;
import org.junit.Test;

public class JsonTest {

	public DateFormat dateFormatter = DataDumpUtils.dateFormatter;
	//JSONParser parser = new JSONParser();
	
	@Test
	public void testStringTab() {
		String original = "Lala\tLala";
		String formatted = DataDumpUtils.getFormattedJSONValue(original, String.class, dateFormatter);
		Assert.assertEquals("\"Lala\\tLala\"", formatted);
		//parser.parse(formatted);
	}
	
	@Test
	public void testStringLf() {
		String original = "Lala\nLala";
		String formatted = DataDumpUtils.getFormattedJSONValue(original, String.class, dateFormatter);
		Assert.assertEquals("\"Lala\\nLala\"", formatted);
	}

	@Test
	public void testStringCrLf() {
		String original = "Lala\r\nLala";
		String formatted = DataDumpUtils.getFormattedJSONValue(original, String.class, dateFormatter);
		Assert.assertEquals("\"Lala\\r\\nLala\"", formatted);
	}

	@Test
	public void testStringQuot() {
		//String r = Matcher.quoteReplacement("\\\"");
		//System.out.println(">>> "+r);
		
		String original = "Lala\"Lala";
		String formatted = DataDumpUtils.getFormattedJSONValue(original, String.class, dateFormatter);
		Assert.assertEquals("\"Lala\\\"Lala\"", formatted);
	}

	@Test
	public void testStringBackslash() {
		String original = "Lala\\Lala";
		String formatted = DataDumpUtils.getFormattedJSONValue(original, String.class, dateFormatter);
		Assert.assertEquals("\"Lala\\\\Lala\"", formatted);
	}
}
