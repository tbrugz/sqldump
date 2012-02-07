package tbrugz.sqldump;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Toolkit;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
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

class BaseInputGUI extends JFrame implements KeyListener, WindowListener {
	static Logger log = Logger.getLogger(BaseInputGUI.class);

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
		addWindowListener(this);
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
		System.exit(0);
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

	public static String PASSECHO_WARN_MESSAGE = "WARN: password will be echoed";
	
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
		if(files==null) {
			if(level==0) {
				log.info("no files deteted");
			}
			return 0;
		}
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
			log.info(syntax+" syntax float locale: "+locale);
			floatFormatter = NumberFormat.getNumberInstance(locale);
		}
		DecimalFormat df = (DecimalFormat) floatFormatter;
		df.setGroupingUsed(false);
		df.applyPattern("###0.000");
		return floatFormatter;
	}
	
	public static String getEqualIgnoreCaseFromList(Collection<String> col, String str) {
		for(String s: col) {
			if(s.equalsIgnoreCase(str)) { return s; }
		}
		return null;
	}

	public static boolean stringListEqualIgnoreCase(List<String> l1, List<String> l2) {
		if(l1.size()!=l2.size()) return false;
		for(int i=0;i<l1.size();i++) {
			if(! l1.get(i).equalsIgnoreCase(l2.get(i))) { return false; }
		}
		return true;
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
