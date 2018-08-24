package com.ftoul.bi.clean.impl;

import com.ftoul.bi.clean.IClean;

/**
 * 默认清洗程序
 * 1、对空对象或者空字符串转换为空字符串
 * 2、对清洗对象左右去空格处理
 * @author xiaohf
 *
 */
public class TrimClean implements IClean {

	public Object clean(Object obj) {
		if (obj == null || "null".equals(obj)) return "";
		return obj.toString().trim();
	}

}
