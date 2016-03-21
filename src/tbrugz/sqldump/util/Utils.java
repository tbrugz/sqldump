package tbrugz.sqldump.util;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Toolkit;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.io.FileFilter;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.xml.bind.DatatypeConverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class RegularFileFilter implements FileFilter {
	
	@Override
	public boolean accept(File f) {
		return !f.getName().startsWith(".") && !f.getName().startsWith("_");
		//return !f.getName().startsWith(".");
	}
}

class BaseInputGUI extends JFrame implements KeyListener, WindowListener {
	private static final long serialVersionUID = 1L;

	static Log log = LogFactory.getLog(BaseInputGUI.class);

	static int width = 500;
	static int height = 80;

	JTextField tf;
	String value;
	RuntimeException throwit = null;
	
	public BaseInputGUI() {
	}
	
	public void doGUI(String message) {
		setTitle("sqldump");
		getContentPane().setLayout(new BorderLayout());
		getContentPane().add(new JLabel(message), BorderLayout.NORTH);
		getContentPane().add(tf, BorderLayout.CENTER);
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		addWindowListener(this);
		setSize(width, height);
		pack(); //?
		
		Toolkit tk = Toolkit.getDefaultToolkit();
		Dimension screenSize = tk.getScreenSize();
		final int screenWidth = screenSize.width;
		final int screenHeight = screenSize.height;
		int actualWidth = getWidth();
		int actualHeight = getHeight();

		// Setup the frame accordingly
		// This is assuming you are extending the JFrame //class
		//this.setSize(WIDTH / 2, HEIGHT / 2);
		this.setLocation((screenWidth - actualWidth)/ 2, (screenHeight - actualHeight) / 2);
		
		setVisible(true);
	}

	@Override
	public void keyTyped(KeyEvent e) {
		if('\n'==e.getKeyChar()) {
			value = tf.getText();
			//removeWindowListener(this); //?
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
			if(throwit!=null) {
				log.warn("throwable detected: exiting");
				throw throwit;
			}
		}
		return value;
	}

	@Override
	public void windowOpened(WindowEvent e) {
	}

	@Override
	public void windowClosing(WindowEvent e) {
	}

	@Override
	public void windowClosed(WindowEvent e) {
		if(value!=null) { return; } 
		log.warn("windowClosed: exiting sqldump");
		throwit = new RuntimeException("windowClosed: exiting sqldump");
		
		//XXXxx: check for permission (java.lang.RuntimePermission exitVM)? exception during ant invocation...
		//System.exit(0);
	}

	@Override
	public void windowIconified(WindowEvent e) {
	}

	@Override
	public void windowDeiconified(WindowEvent e) {
	}

	@Override
	public void windowActivated(WindowEvent e) {
	}

	@Override
	public void windowDeactivated(WindowEvent e) {
	}
	
}

class TextInputGUI extends BaseInputGUI {
	private static final long serialVersionUID = 1L;

	public TextInputGUI(String message) {
		tf = new JTextField(15);
		tf.addKeyListener(this);
		doGUI(message);
	}
}

class PasswordInputGUI extends BaseInputGUI {
	private static final long serialVersionUID = 1L;

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

/*
 * TODO: move getProp* functions to another class
 */
public class Utils {
	
	static final Log log = LogFactory.getLog(Utils.class);
	
	static final Locale localeEN = new Locale("en");
	
	/*
	 * http://snippets.dzone.com/posts/show/91
	 * http://stackoverflow.com/questions/1515437/java-function-for-arrays-like-phps-join
	 */
	public static String join(Collection<?> s, String delimiter) {
		return join(s, delimiter, null);
	}
	
	/*@Deprecated
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
	}*/

	public static String join(Collection<?> s, String delimiter, StringDecorator decorator) {
		StringBuilder buffer = new StringBuilder();
		if(s==null) { return null; }
		Iterator<?> iter = s.iterator();
		while (iter.hasNext()) {
			Object elem = iter.next();
			String sElem = elem!=null?elem.toString():null;
			if(decorator!=null) {
				buffer.append(decorator.get(sElem));
			}
			else {
				buffer.append(sElem);
			}

			if (iter.hasNext()) {
				buffer.append(delimiter);
			}
		}
		return buffer.toString();
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
				if(!parent.mkdirs()) {
					log.warn("error creating dirs: "+parent.getAbsolutePath());
				}
			}
		}
		else {
			log.warn("null parent dir? file: "+f.getAbsolutePath());
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

	/** returns Boolean, so that return can be null */
	public static Boolean getPropBoolean(Properties prop, String key) {
		return getPropBoolean(prop, key, null);
	}
	
	/** returns Boolean, so that return can be null */
	public static Boolean getPropBoolean(Properties prop, String key, Boolean defaultValue) {
		String value = prop.getProperty(key);
		if(value==null) { return defaultValue; }
		return "true".equals(value.trim());
	}
	
	public static Integer getPropInt(Properties prop, String key) {
		return getPropInt(prop, key, null);
	}
	
	public static Integer getPropInt(Properties prop, String key, Integer defaultValue) {
		String str = prop.getProperty(key);
		try {
			int i = Integer.parseInt(str);
			return i;
		}
		catch(Exception e) {
			return defaultValue;
		}
	}

	public static Long getPropLong(Properties prop, String key) {
		return getPropLong(prop, key, null);
	}
	
	public static Long getPropLong(Properties prop, String key, Long defaultValue) {
		String str = prop.getProperty(key);
		try {
			long l = Long.parseLong(str);
			return l;
		}
		catch(Exception e) {
			return defaultValue;
		}
	}
	
	public static Double getPropDouble(Properties prop, String key) {
		return getPropDouble(prop, key, null);
	}
	
	public static Double getPropDouble(Properties prop, String key, Double defaultValue) {
		String str = prop.getProperty(key);
		try {
			double d = Double.parseDouble(str);
			return d;
		}
		catch(Exception e) {
			return defaultValue;
		}
	}

	public static boolean propertyExists(Properties prop, String key) {
		return prop.getProperty(key)!=null;
	}
	
	public static List<String> getStringListFromProp(Properties prop, String key, String delimiter) {
		String strings = prop.getProperty(key);
		return getStringList(strings, delimiter);
	}

	public static List<String> getStringListFromProp(Properties prop, String key, String delimiter, Object[] allowedValues) {
		String strings = prop.getProperty(key);
		return getStringList(strings, delimiter, allowedValues);
	}
	
	public static List<String> getStringList(String strings, String delimiter) {
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

	public static List<String> getStringList(String strings, String delimiter, Object[] allowedValues) {
		List<String> sl = getStringList(strings, delimiter);
		List<String> sret = new ArrayList<String>();
		if(sl==null) return null;
		//Arrays.sort(allowedValues);
		
		for(String s: sl) {
			boolean added = false;
			for(Object sok: allowedValues) {
				if(sok.toString().equals(s)) {
					sret.add(s);
					added = true;
					break;
				}
			}
			if(!added) {
				log.warn("not allowed value: "+s);
			}
		}
		
		return sret;
	}
	
	public static List<String> splitStringWithTrim(String string, String delimiter) {
		List<String> ret = new ArrayList<String>();
		String[] retArr = string.split(delimiter);
		for(String s: retArr) {
			ret.add(s.trim());
		}
		return ret;
	}

	static final String PASSECHO_WARN_MESSAGE = "WARN: password will be echoed";
	
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
			return Utils.readTextIntern(message+"["+PASSECHO_WARN_MESSAGE+"] ", "");
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
		StringBuilder sb = new StringBuilder();
		InputStream is = System.in;
		try {
			int read = 0;
			while((read = is.read()) != -1) {
				if(read==13) { break; }
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

	public static Class<?> getClassWithinPackages(String className, String... packages) {
		Exception ex = null;
		if(packages==null || packages.length==0) {
			packages = new String[]{null};
		}
		for(String pkg: packages) {
			try {
				String prepend = (pkg!=null&&pkg.length()>0) ? pkg+"." : "";
				String classToFind = prepend+className;
				Class<?> c = Class.forName(classToFind);
				return c;
			} catch (ClassNotFoundException e) {
				if(ex==null) { ex = e; }
			}
		}
		log.debug("class not found: "+className+" [ex: "+ex+"]",ex);
		return null;
	}
	
	public static int deleteDirRegularContents(File f) {
		log.debug("deleting regular files from dir: "+f.getAbsolutePath());
		int delCount = deleteDirRegularContents(f, 0);
		if(delCount==0) {
			log.debug("no files deteted");
		}
		else {
			log.debug(delCount+" files deteted");
		}
		return delCount;
	}
	
	static int deleteDirRegularContents(File f, int level) {
		File[] files = f.listFiles(new RegularFileFilter());
		if(files==null) {
			return 0;
		}
		int delCount = 0;
		for(File ff: files) {
			if(ff.isFile()) {
				log.debug("file to delete: "+ff);
				boolean deleted = ff.delete();
				if(!deleted) { log.warn("can't delete file: "+ff); }
				else { delCount++; }
			}
			else if(ff.isDirectory()) {
				//XXXxx: deleteDirRegularContents: recurse int subdirs
				delCount += deleteDirRegularContents(ff, level+1);
				log.debug("dir to delete: "+ff);
				boolean deleted = ff.delete();
				if(!deleted) { log.warn("can't delete dir: "+ff); }
				else { delCount++; }
			}
		}
		return delCount;
	}
	
	public static List<String> getKeysStartingWith(Properties prop, String startStr) {
		List<String> ret = new ArrayList<String>();
		if(ParametrizedProperties.isUseSystemProperties()) {
			prop.putAll(System.getProperties());
		}
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
			//System.out.println("Util: show sys prop");
			Map<Object, Object> m = new TreeMap<Object, Object>(System.getProperties());
			log.debug("system properties:");
			for(Entry<Object, Object> entry: m.entrySet()) {
				log.debug("\t"+entry.getKey()+": "+entry.getValue());
			}
			log.debug("end system properties");
		}
	}

	public static void logEnvironment() {
		if(log.isDebugEnabled()) {
			log.debug("os: "+System.getProperty("os.name")
					+" "+System.getProperty("os.version")
					+" ("+System.getProperty("os.arch")+")"
					+"; java.runtime: "+System.getProperty("java.runtime.version")
					);
		}
		
	}
	
	public static void logMemoryUsage() {
		if(log.isDebugEnabled()) {
			// http://stackoverflow.com/questions/1058991/how-to-monitor-java-memory-usage
			Runtime rt = Runtime.getRuntime();
			long totalMB = rt.totalMemory() / 1024 / 1024;
			long usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
			log.debug("used memory: "+usedMB+"Mb [total: "+totalMB+"Mb]");
		}
	}
	
	//XXX: remove 'syntax' param?
	public static NumberFormat getFloatFormatter(String floatLocale, String floatFormat, String syntax) {
		NumberFormat floatFormatter = null;
		if(floatLocale==null) {
			floatFormatter = NumberFormat.getNumberInstance(localeEN);
		}
		else {
			Locale locale = new Locale(floatLocale);
			log.info(syntax+" syntax float locale: "+locale);
			floatFormatter = NumberFormat.getNumberInstance(locale);
		}
		DecimalFormat df = (DecimalFormat) floatFormatter;
		df.setGroupingUsed(false);
		if(floatFormat==null) {
			floatFormat = "###0.000"; 
		}
		df.applyPattern(floatFormat);
		return floatFormatter;
	}
	
	public static String getEqualIgnoreCaseFromList(Collection<String> col, String str) {
		for(String s: col) {
			if(s==null) { continue; }
			if(s.equalsIgnoreCase(str)) { return s; }
		}
		return null;
	}

	public static boolean stringListEqualIgnoreCase(List<String> l1, List<String> l2) {
		if(l1.size()!=l2.size()) { return false; }
		for(int i=0;i<l1.size();i++) {
			if(! l1.get(i).equalsIgnoreCase(l2.get(i))) { return false; }
		}
		return true;
	}
	
	public static List<String> newStringList(String... strings) {
		List<String> ret = new ArrayList<String>();
		for(String s: strings) {
			ret.add(s);
		}
		return ret;
	}
	
	static boolean equalsConsiderNull(String s1, String s2) {
		if(s1==null) {
			return (s2==null);
		}
		else if(s2==null) { return false; }
		return s1.equals(s2);
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

	public static Object getClassInstance(String className, String... defaultPackages) {
		Class<?> c = getClassWithinPackages(className, defaultPackages);
		if(c==null) {
			log.warn("class not found: "+className);
			return null;
		}
		Object o = getClassInstance(c);
		if(o==null) {
			log.warn("couldn't instantiate object from class: "+className);
		}
		return o;
	}
	
	public static String countByKeyString(Map<?,?> map) {
		StringBuilder sb = new StringBuilder();
		for(Entry<?,?> entry: map.entrySet()) {
			sb.append((sb.length()>0?";":"")+"#"+entry.getKey()+"="+entry.getValue());
		}
		return sb.toString();
	}

	//see: http://stackoverflow.com/questions/469695/decode-base64-data-in-java/2054226#2054226
	public static String parseBase64(String str) throws UnsupportedEncodingException {
		byte[] arr = DatatypeConverter.parseBase64Binary(str);
		return new String(arr, "UTF-8");
	}

	public static String printBase64(String str) {
		return DatatypeConverter.printBase64Binary(str.getBytes());
	}
	
	static Set<String> deprecatedKeysWarned = new HashSet<String>();
	
	public static String getPropWithDeprecated(Properties p, String key, String deprecatedKey, String defaultValue) {
		String ret = defaultValue;
		if(Utils.propertyExists(p, deprecatedKey)) {
			if(!deprecatedKeysWarned.contains(deprecatedKey)) { 
				log.warn("deprecated prop '"+deprecatedKey+"' present - use '"+key+"' instead");
				deprecatedKeysWarned.add(deprecatedKey);
			}
			ret = p.getProperty(deprecatedKey, defaultValue);
		}
		ret = p.getProperty(key, ret);
		return ret;
	}

	public static boolean getPropBoolWithDeprecated(Properties p, String key, String deprecatedKey, boolean defaultValue) {
		boolean ret = defaultValue;
		if(Utils.propertyExists(p, deprecatedKey)) {
			if(!deprecatedKeysWarned.contains(deprecatedKey)) { 
				log.warn("deprecated prop '"+deprecatedKey+"' present - use '"+key+"' instead");
				deprecatedKeysWarned.add(deprecatedKey);
			}
			ret = Utils.getPropBool(p, deprecatedKey, defaultValue);
		}
		ret = Utils.getPropBool(p, key, ret);
		return ret;
	}

	public static Long getPropLongWithDeprecated(Properties p, String key, String deprecatedKey, Long defaultValue) {
		Long ret = defaultValue;
		if(Utils.propertyExists(p, deprecatedKey)) {
			if(!deprecatedKeysWarned.contains(deprecatedKey)) { 
				log.warn("deprecated prop '"+deprecatedKey+"' present - use '"+key+"' instead");
				deprecatedKeysWarned.add(deprecatedKey);
			}
			ret = Utils.getPropLong(p, deprecatedKey, defaultValue);
		}
		ret = Utils.getPropLong(p, key, ret);
		return ret;
	}
	
}
