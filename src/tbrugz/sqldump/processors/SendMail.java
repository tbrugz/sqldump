package tbrugz.sqldump.processors;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.AbstractProcessor;
import tbrugz.sqldump.def.ProcessingException;

public class SendMail extends AbstractProcessor {
	static final Log log = LogFactory.getLog(SendMail.class);
	
	public static final String PROP_PREFIX = "sqldump.sendmail";
	
	public static final String SUFFIX_FROM = "from";
	public static final String SUFFIX_TO = "to";
	public static final String SUFFIX_SUBJECT = "subject";
	public static final String SUFFIX_BODY = "body";
	
	public static final String SMTP_HOST = "mail.smtp.host";
	public static final String SMTP_PORT = "mail.smtp.port";
	
	String propPrefix = PROP_PREFIX;
	
	String from;
	String to;
	String subject;
	String body;
	
	String smtpHost;
	String smtpPort;
	
	@Override
	public void setProperties(Properties prop) {
		from = prop.getProperty(propPrefix+"."+SUFFIX_FROM);
		to = prop.getProperty(propPrefix+"."+SUFFIX_TO);
		subject = prop.getProperty(propPrefix+"."+SUFFIX_SUBJECT);
		body = prop.getProperty(propPrefix+"."+SUFFIX_BODY);
		
		smtpHost = prop.getProperty(propPrefix+"."+SMTP_HOST);
		smtpPort = prop.getProperty(propPrefix+"."+SMTP_PORT);
	}

	@Override
	public void process() {
		Properties mailSessionProps = new Properties();
		if(smtpHost!=null) {
			mailSessionProps.setProperty(SMTP_HOST, smtpHost);
		}
		if(smtpPort!=null) {
			mailSessionProps.setProperty(SMTP_PORT, smtpPort);
		}
		Session session = Session.getDefaultInstance(mailSessionProps, null);

		try {
			Message msg = new MimeMessage(session);
			msg.setFrom(new InternetAddress(from));
			msg.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
			msg.setSubject(subject);
			msg.setText(body);
			Transport.send(msg);
		} catch (MessagingException e) {
			log.warn("Error sending email: "+e);
			log.debug("Error sending email", e);
			if(failonerror) {
				throw new ProcessingException(e);
			}
		}
	}

}
