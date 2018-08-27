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
	 * 清洗类型 COL按照字段清洗，FIX: 按照定长文件格式清洗
	 */
	private String type = "COL";
	
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
	
	/**
	 * 初始化根节点的其他属性
	 * @throws Exception
	 */
	private void initParam() throws Exception {
		sepStr = getAttributeByRoot(Constant.SEPSTR,"\001");
		type=getAttributeByRoot(Constant.TYPE,"COL");
		if (rootElement.hasAttribute(Constant.FIELDCNT)) {
			fieldCnt = Integer.parseInt(rootElement.getAttribute(Constant.FIELDCNT));
		} else {
			throw new Exception("not found attribute:" + Constant.FIELDCNT);
		}
	}
	
	/**
	 * 根据属性名获取根节点的属性值
	 * @param name
	 * @param defaultValue 默认值
	 * @return
	 */
	public String getAttributeByRoot(String name, String defaultValue) {
		if (rootElement.hasAttribute(name)) {
			return rootElement.getAttribute(name);
		}
		return defaultValue;
	}
	
	public String parseRow(String row) throws Exception {
		if ("FIX".equalsIgnoreCase(type)) {
			return parseFixedRows(row);
		} else {
			return parseColRow(row);
		}
	}

	/**
	 * 根据分隔符参数解析每一行数据，并根据配置文件的配置返回解析的结果
	 * 
	 * @param row
	 *            需要解析的行
	 * @return <code>String<code>
	 */
	public String parseColRow(String row) {
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
				IClean clean = getClean(n);
				
				sb.append(clean.clean(cols[index-1]));
			}
			sb.append(sepStr);
		}
		if (sb.toString().endsWith(sepStr)) {
			return sb.substring(0, sb.length() - sepStr.length());
		}
		return sb.toString();

	}
	
	/**
	 * 清洗每列固定长度的数据源，固定长度的开始位置和结束位置定义在index属性中
	 * @param row 需要解析的行
	 * @return <code>String<code>
	 */
	public String parseFixedRows(String row) {
		StringBuffer sb = new StringBuffer();

		NodeList nodes = rootElement.getElementsByTagName(Constant.COLUMN_NAM);
		for (int ind = 0; ind < nodes.getLength(); ind++) {
			Element n = (Element) nodes.item(ind);
			String indexDesc = n.getAttribute(Constant.INDEX_ATTR_NAM);
			String[] indexs = indexDesc.split("-|,");
			if (indexs!= null && indexs.length == 2) {
				int begin = Integer.parseInt(indexs[0]);
				int end = Integer.parseInt(indexs[1]);
				if (end <= row.length()) {
					end = row.length();
				}
				String sourceValue = row.substring(begin, end);
				IClean clean = getClean(n);
				sb.append(clean.clean(sourceValue));
				
			}
		}
		if (sb.toString().endsWith(sepStr)) {
			return sb.substring(0, sb.length() - sepStr.length());
		}
		return sb.toString();
	}
	
	/**
	 * 根据清洗规则节点获取清洗对象
	 * @param element
	 * @return
	 */
	private IClean getClean(Element element) {
		IClean clean = null;
		if (element.hasAttribute("clean")) {
			// 如果有定义clean属性，则根据clean属性找到清洗程序来处理
			clean = CleanMap.get(element.getAttribute("clean"));
			if (element.hasAttribute("params") && clean != null) {
				String attr = element.getAttribute("params");
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
		return clean;
	}

}
