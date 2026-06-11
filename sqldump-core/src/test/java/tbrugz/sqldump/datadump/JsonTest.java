package tbrugz.sqldump.datadump;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.junit.Assert;
import org.junit.Test;

public class JsonTest {

	public DateFormat dateFormatter = new SimpleDateFormat(DataDumpUtils.DATE_FORMATTER_PATTERN);
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

	@Test
	public void testBooleanValue() {
		Boolean original = true;
		String formatted = DataDumpUtils.getFormattedJSONValue(original, Boolean.class, dateFormatter);
		Assert.assertEquals("true", formatted);
	}

	@Test
	public void testIntegerValue() {
		Integer original = 1234;
		String formatted = DataDumpUtils.getFormattedJSONValue(original, Integer.class, dateFormatter);
		Assert.assertEquals("1234", formatted);
	}

	@Test
	public void testDoubleValue() {
		Double original = 1234.56;
		String formatted = DataDumpUtils.getFormattedJSONValue(original, Double.class, dateFormatter);
		Assert.assertEquals("1234.56", formatted);
	}

}
