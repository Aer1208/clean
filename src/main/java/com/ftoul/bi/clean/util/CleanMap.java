package com.ftoul.bi.clean.util;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.ftoul.bi.clean.IClean;
import com.ftoul.bi.clean.impl.TrimClean;

/**
 * 注册所有的清洗程序
 * @author xiaohf
 *
 */
public class CleanMap {

	public static Map<String, IClean> cleans = new HashMap<String, IClean>();
	private static Properties prop;
	
	static {
		
		prop = new Properties();
		try {
			prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("clean.properties"));
			String basePath = getPropValue("clean_base_path");
			if (!basePath.endsWith("/")) {
				basePath += "/";
			}
			String cleanFile = prop.getProperty("clean_conf_file");
			initCleans(basePath + cleanFile);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
		
	}
	
	public static String getPropValue(String key) {
		return prop.getProperty(key);
	}
	
	/**
	 * 根据清洗作业ID获取清洗程序，如果没有则获取<code>TrimClean</code>
	 * @param cleanId
	 * @return
	 */
	public static IClean get(String cleanId) {
		IClean clean = cleans.get(cleanId);
		if (clean == null) return new TrimClean();
		return clean;
	}

	private static void initCleans(String cleanPath) throws ParserConfigurationException, SAXException, IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(cleanPath),conf);
		Path path = new Path(cleanPath);
		FSDataInputStream confInputStream = fs.open(path);
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document document = db.parse(confInputStream);
		Element rootElement = document.getDocumentElement();
		NodeList cleanList = rootElement.getElementsByTagName("clean");
		for (int i=0; i < cleanList.getLength(); i++) {
			Element node = (Element) cleanList.item(i);
			if (node.hasAttribute("id") && node.hasAttribute("class")) {
				String cleanId = node.getAttribute("id");
				String clazzStr = node.getAttribute("class");
				try {
					Class<?> clazz = Class.forName(clazzStr);
					IClean cleanObj = (IClean) clazz.newInstance();
					cleans.put(cleanId, cleanObj);
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} 
			}
		}
	}
}
