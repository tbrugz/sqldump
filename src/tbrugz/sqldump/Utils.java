package tbrugz.sqldump;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Toolkit;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import java.io.FileFilter;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

import org.apache.log4j.Logger;

class RegularFileFilter implements FileFilter {
	
	@Override
	public boolean accept(File f) {
		return !f.getName().startsWith(".") && !f.getName().startsWith("_");
		//return !f.getName().startsWith(".");
	}
}

class BaseInputGUI extends JFrame implements KeyListener {
	static int width = 200;
	static int height = 100;

	JTextField tf;
	String value;
	
	public BaseInputGUI() {
	}
	
	public void doGUI(String message) {
		setTitle("sqldump");
		getContentPane().setLayout(new FlowLayout());
		getContentPane().add(new JLabel(message));
		getContentPane().add(tf);
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		setSize(200, 100);
		
		Toolkit tk = Toolkit.getDefaultToolkit();
		Dimension screenSize = tk.getScreenSize();
		final int WIDTH = screenSize.width;
		final int HEIGHT = screenSize.height;
		// Setup the frame accordingly
		// This is assuming you are extending the JFrame //class
		//this.setSize(WIDTH / 2, HEIGHT / 2);
		this.setLocation((WIDTH - width)/ 2, (HEIGHT - height) / 2);
		
		setVisible(true);
	}

	@Override
	public void keyTyped(KeyEvent e) {
		if('\n'==e.getKeyChar()) {
			value = tf.getText();
			setVisible(false);
			this.dispose();
		}
	}

	@Override
	public void keyPressed(KeyEvent e) {
	}

	@Override
	public void keyReleased(KeyEvent e) {
	}
	
	public String getText() {
		while(value==null) {
			Thread.yield();
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return value;
	}
	
}

class TextInputGUI extends BaseInputGUI {
	//JTextField tf;
	//String value;
	
	public TextInputGUI(String message) {
		tf = new JTextField(15);
		tf.addKeyListener(this);
		doGUI(message);
	}
}

class PasswordInputGUI extends BaseInputGUI {
	//JPasswordField pf;
	
	public PasswordInputGUI(String message) {
		//tf.removeKeyListener(this);
		tf = new JPasswordField(15);
		tf.addKeyListener(this);
		doGUI(message);
	}

	@Override
	public void keyTyped(KeyEvent e) {
		if('\n'==e.getKeyChar()) {
			//System.out.println("Enter! (pi)"+e.getKeyChar()+"; "+e);
			value = String.valueOf(((JPasswordField)tf).getPassword());
			setVisible(false);
			this.dispose();
		}
	}
}

public class Utils {
	
	static Logger log = Logger.getLogger(Utils.class);
	
	//see: http://download.oracle.com/javase/1.5.0/docs/api/java/text/SimpleDateFormat.html
	public static DateFormat dateFormatter = new SimpleDateFormat("''yyyy-MM-dd''");
	public static NumberFormat floatFormatterSQL = null;
	//public static NumberFormat floatFormatterBR = null;
	public static NumberFormat longFormatter = null;
	
	static {
		floatFormatterSQL = NumberFormat.getNumberInstance(Locale.ENGLISH); //new DecimalFormat("##0.00#");
		DecimalFormat df = (DecimalFormat) floatFormatterSQL;
		df.setGroupingUsed(false);
		df.applyPattern("###0.00#");
	}

	/*static {
		floatFormatterBR = NumberFormat.getNumberInstance();
		DecimalFormat df = (DecimalFormat) floatFormatterBR;
		df.setGroupingUsed(false);
		df.applyPattern("###0.000");
	}*/

	static {
		longFormatter = NumberFormat.getNumberInstance();
		DecimalFormat df = (DecimalFormat) longFormatter;
		df.setGroupingUsed(false);
		df.setMaximumIntegerDigits(20); //E??
		df.applyPattern("###0");//87612933000118
	}
	
	/*
	 * http://snippets.dzone.com/posts/show/91
	 * http://stackoverflow.com/questions/1515437/java-function-for-arrays-like-phps-join
	 */
	public static String join(Collection<?> s, String delimiter) {
		return join(s, delimiter, null, false);
	}
	
	public static String join(Collection<?> s, String delimiter, String enclosing, boolean enclosingJustForNonNulls) {
		StringBuffer buffer = new StringBuffer();
		Iterator<?> iter = s.iterator();
		while (iter.hasNext()) {
			Object elem = iter.next();
			if(enclosing!=null && !"".equals(enclosing)) {
				if(enclosingJustForNonNulls && elem==null) {
					buffer.append(elem);
				}
				else {
					buffer.append(enclosing+elem+enclosing);
				}
			}
			else {
				buffer.append(elem);
			}

			if (iter.hasNext()) {
				buffer.append(delimiter);
			}
		}
		return buffer.toString();
	}
	
	static String DEFAULT_ENCLOSING = "'";
	static String DOUBLEQUOTE = "\"";
	
	public static String join4sql(Collection<?> s, DateFormat df, String delimiter) {
		StringBuffer buffer = new StringBuffer();
		Iterator<?> iter = s.iterator();
		while (iter.hasNext()) {
			buffer.append(getFormattedSQLValue(iter.next(), df));

			if (iter.hasNext()) {
				buffer.append(delimiter);
			}
		}
		return buffer.toString();
	}
	
	public static String getFormattedSQLValue(Object elem, DateFormat df) {
		if(elem == null) {
			return null;
		}
		else if(elem instanceof String) {
			/* XXX?: String escaping? "\n, \r, ', ..."
			 * see: http://www.orafaq.com/wiki/SQL_FAQ#How_does_one_escape_special_characters_when_writing_SQL_queries.3F 
			 */
			elem = ((String) elem).replaceAll("'", "''");
			return DEFAULT_ENCLOSING+elem+DEFAULT_ENCLOSING;
		}
		else if(elem instanceof Date) {
			return df.format((Date)elem);
		}
		else if(elem instanceof Float) {
			return floatFormatterSQL.format((Float)elem);
		}
		else if(elem instanceof Double) {
			//log.debug("format:: "+elem+" / "+floatFormatterSQL.format((Double)elem));
			return floatFormatterSQL.format((Double)elem);
		}
		/*else if(elem instanceof Integer) {
			return String.valueOf(elem);
		}*/

		return String.valueOf(elem);
	} 
	
	public static String getFormattedJSONValue(Object elem, DateFormat df) {
		if(elem == null) {
			return null;
		}
		else if(elem instanceof String) {
			elem = ((String) elem).replaceAll(DOUBLEQUOTE, "&quot;");
			return DOUBLEQUOTE+elem+DOUBLEQUOTE;
		}
		else if(elem instanceof Date) {
			//XXXdone: JSON dateFormatter?
			return df.format((Date)elem);
		}
		else if(elem instanceof Long) {
			//log.warn("long: "+(Long)elem+"; "+longFormatter.format((Long)elem));
			return longFormatter.format((Long)elem);
		}

		return String.valueOf(elem);
	} 

	public static String getFormattedCSVValue(Object elem, NumberFormat floatFormatter, String separator, String nullValue) {
		if(elem == null) {
			return nullValue;
		}
		else if(elem instanceof Double) {
			return floatFormatter.format((Double)elem);
		}

		if(separator==null) {
			return String.valueOf(elem);
		}
		else {
			return String.valueOf(elem).replaceAll(separator, "");
		}
	} 

	public static String normalizeEnumStringConstant(String strEnumConstant) {
		return strEnumConstant.replace(' ', '_');
	}

	/*
	 * creates dir for file if it doesn't already exists
	 */
	public static void prepareDir(File f) {
		File parent = f.getParentFile();
		if(parent!=null) {
			if(!parent.exists()) {
				parent.mkdirs();
			}
		}
	}
	
	/*
	public static String denormalizeEnumStringConstant(String strEnumConstant) {
		return strEnumConstant.replace('_', ' ');
	}
	*/
	
	public static boolean getPropBool(Properties prop, String key) {
		return getPropBool(prop, key, false);
	}

	public static boolean getPropBool(Properties prop, String key, boolean defaultValue) {
		String value = prop.getProperty(key);
		if(value==null) { return defaultValue; }
		return "true".equals(value.trim());
	}
	
	public static Long getPropLong(Properties prop, String key) {
		String str = prop.getProperty(key);
		try {
			long l = Long.parseLong(str);
			return l;
		}
		catch(Exception e) {
			return null;
		}
	}

	public static List<String> getStringListFromProp(Properties prop, String key, String delimiter) {
		String strings = prop.getProperty(key);
		if(strings!=null) {
			List<String> ret = new ArrayList<String>();
			String[] retArr = strings.split(delimiter);
			for(String s: retArr) {
				ret.add(s.trim());
			}
			return ret;
		}
		return null;
	}
	
	public static String readPassword(String message) {
		Console cons = System.console();
		//log.info("console: "+cons);
		char[] passwd;
		if (cons != null) {
			System.out.print(message);
			passwd = cons.readPassword(); //"[%s]", message
			return new String(passwd);
			//java.util.Arrays.fill(passwd, ' ');
		}
		else {
			//XXX: System.console() doesn't work in Eclipse - https://bugs.eclipse.org/bugs/show_bug.cgi?id=122429
			return Utils.readTextIntern(message, "");
		}
	}

	public static String readText(String message) {
		Console cons = System.console();
		//log.info("console: "+cons);
		String text;
		if (cons != null) {
			System.out.print(message);
			text = cons.readLine();
			return text;
		}
		else {
			//XXX: System.console() doesn't work in Eclipse - https://bugs.eclipse.org/bugs/show_bug.cgi?id=122429
			return Utils.readTextIntern(message, "");
		}
	}
	
	/*public static String readPasswordSwing(String message) {
		Console cons = System.console();
		//log.info("console: "+cons);
		char[] passwd;
		if (cons != null) {
			System.out.print(message);
			passwd = cons.readPassword(); //"[%s]", message
			return new String(passwd);
			//java.util.Arrays.fill(passwd, ' ');
		}
		else {
			//XXX: System.console() doesn't work in Eclipse - https://bugs.eclipse.org/bugs/show_bug.cgi?id=122429
			return Utils.readPasswordIntern(message, "");
		}
	}*/
	
	static String readTextIntern(String message, String replacer) {
		System.out.print(message);
		StringBuffer sb = new StringBuffer();
		InputStream is = System.in;
		try {
			int read = 0;
			while((read = is.read()) != -1) {
				if(read==13) break;
				char c = (char) (read & 0xff);
				sb.append(c);
				System.out.print(replacer);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}
	
	public static Object getClassInstance(Class<?> c) {
		try {
			return c.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Object getClassInstance(String className) {
		try {
			Class<?> c = Class.forName(className);
			return getClassInstance(c);
		} catch (ClassNotFoundException e) {
			log.debug("class not found: "+e.getMessage(), e);
		}
		return null;
	}
	
	public static void deleteDirRegularContents(String s) {
		deleteDirRegularContents(s, 0);
	}
	
	static int deleteDirRegularContents(String s, int level) {
		if(level==0) {
			log.info("deleting regular files from dir: "+s);
		}
		File f = new File(s);
		File files[] = f.listFiles(new RegularFileFilter());
		if(files==null) return 0;
		int delCount = 0;
		for(File ff: files) {
			if(ff.isFile()) {
				log.debug("file to delete: "+ff);
				ff.delete();
				delCount++;
			}
			else if(ff.isDirectory()) {
				//XXXxx: deleteDirRegularContents: recurse int subdirs
				delCount += deleteDirRegularContents(ff.getAbsolutePath(), level+1);
				log.debug("dir to delete: "+ff);
				ff.delete();
				delCount++;
			}
		}
		if(level==0) {
			log.info(delCount+" files deteted");
		}
		return delCount;
	}
	
	public static List<String> getKeysStartingWith(Properties prop, String startStr) {
		List<String> ret = new ArrayList<String>();
		for(Object o: prop.keySet()) {
			String s = (String) o;
			if(s.startsWith(startStr)) {
				ret.add(s);
			}
		}
		return ret;
	}
	
	public static String readTextGUI(String message) {
		BaseInputGUI tig = new TextInputGUI(message);
		return tig.getText();
	}
	
	public static String readPasswordGUI(String message) {
		BaseInputGUI pig = new PasswordInputGUI(message);
		return pig.getText();
	}
	
	public static void showSysProperties() {
		if(log.isDebugEnabled()) {
			System.out.println("Util: show sys prop");
			Map<Object, Object> m = new TreeMap<Object, Object>(System.getProperties());
			log.debug("system properties:");
			for(Object key: m.keySet()) {
				log.debug("\t"+key+": "+m.get(key));
			}
			log.debug("end system properties");
		}
	}
	
	public static NumberFormat getFloatFormatter(String floatLocale, String syntax) {
		NumberFormat floatFormatter = null;
		if(floatLocale==null) {
			floatFormatter = NumberFormat.getNumberInstance();
		}
		else {
			Locale locale = new Locale(floatLocale);
			log.info(syntax+" syntax locale: "+locale);
			floatFormatter = NumberFormat.getNumberInstance(new Locale(floatLocale));
		}
		DecimalFormat df = (DecimalFormat) floatFormatter;
		df.setGroupingUsed(false);
		df.applyPattern("###0.000");
		return floatFormatter;
	}
	
	public static void main(String[] args) {
		//String value = PasswordInputGUI.getPassword("pass: ");
		
		BaseInputGUI tig = new TextInputGUI("user: ");
		String user = tig.getText();
		System.out.println("user = "+user);
		
		BaseInputGUI pig = new PasswordInputGUI("pass for user "+user+": ");
		String pass = pig.getText();
		System.out.println("pass = "+pass);

		//String s = readPasswordIntern("pass: ", "*");
		//System.out.println("s = "+s);
	}
	
}
