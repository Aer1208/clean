package com.ftoul.bi.clean.util;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.ftoul.bi.clean.IClean;
import com.ftoul.bi.clean.impl.TrimClean;


/**
 * 数据源接口字段解析类
 * @author xiaohf
 *
 */
public class RowParser {
	/**
	 * 读取文件分隔符
	 */
	private String sepStr="\001";
	/**
	 * 数据源字段记录数
	 */
	private int fieldCnt=0; 
	
	/**
	 * 解析的配置文件
	 */
	private String xmlFile;
	private Document document = null;
	private Element rootElement = null;
	
	public RowParser(String xmlFile) throws Exception {
		if (!xmlFile.startsWith("hdfs://")) {
			String basePath = CleanMap.getPropValue("clean_base_path");
			if (!basePath.endsWith("/")) {
				basePath += "/";
			}
			xmlFile = basePath + xmlFile;
		}
		this.xmlFile = xmlFile;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(this.xmlFile),conf);
		Path path = new Path(this.xmlFile);
		FSDataInputStream confInputStream = fs.open(path);
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		this.document = dbf.newDocumentBuilder().parse(confInputStream);
		this.rootElement = document.getDocumentElement();
		initParam();
	}
	
	private void initParam() throws Exception {
		if(rootElement.hasAttribute(Constant.SEPSTR)) {
			sepStr = rootElement.getAttribute(Constant.SEPSTR);
		}
		if (rootElement.hasAttribute(Constant.FIELDCNT)) {
			fieldCnt = Integer.parseInt(rootElement.getAttribute(Constant.FIELDCNT));
		} else {
			throw new Exception("not found attribute:" + Constant.FIELDCNT);
		}
	}

	/**
	 * 根据分隔符参数解析每一行数据，并根据配置文件的配置返回解析的结果
	 * 
	 * @param row
	 *            需要解析的行
	 * @return <code>String<code>
	 */
	public String parseRow(String row) {
		String[] cols = row.split(sepStr, fieldCnt);
		StringBuffer sb = new StringBuffer();

		NodeList nodes = rootElement.getElementsByTagName(Constant.COLUMN_NAM);
		for (int ind = 0; ind < nodes.getLength(); ind++) {
			Element n = (Element) nodes.item(ind);
			String type = n.getAttribute(Constant.TYPE_ATTR_NAM);
			int index = Integer.parseInt(n.getAttribute(Constant.INDEX_ATTR_NAM));
			if ("bit".equals(type)) {
				sb.append(cols[index-1].equals("true")?1:0);
			} else {
				IClean clean = null;
				if (n.hasAttribute("clean")) {
					// 如果有定义clean属性，则根据clean属性找到清洗程序来处理
					clean = CleanMap.get(n.getAttribute("clean"));
					if (n.hasAttribute("params") && clean != null) {
						String attr = n.getAttribute("params");
						if (attr != null && !"".equals(attr)) {
							String[] attrs = attr.split(";");
							for (String attrPair : attrs) {
								Matcher matcher = Pattern.compile("([^=]+)=([^=]+)").matcher(attrPair);
								if (matcher.matches()) {
									// 如果属性对是以 attr=value形式定义，则认为有效，为清洗对象增加该属性
									String properties = matcher.group(1);
									String value = matcher.group(2);
									try {
										Class<?> clazz = clean.getClass();
										Field field = clazz.getDeclaredField(properties);
										field.setAccessible(true);
										field.set(clean, value);
									} catch (NoSuchFieldException e) {
									} catch (SecurityException e) {
									} catch (IllegalArgumentException e) {
									} catch (IllegalAccessException e) {
									}
									
								}
							}
						}
					}
				}
				
				if (clean == null) {
					clean = new TrimClean();
				}
				
				sb.append(clean.clean(cols[index-1]));
			}
			sb.append(sepStr);
		}
		if (sb.toString().endsWith(sepStr)) {
			return sb.substring(0, sb.length() - 1);
		}
		return sb.toString();

	}

}
