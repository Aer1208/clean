package com.ftoul.bi.clean.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.ftoul.bi.clean.IClean;

public class DateFormatClean implements IClean {
	
	private String fromFormat;
	private String toFormat;

	public Object clean(Object obj) {
		
		SimpleDateFormat formSdf = new SimpleDateFormat(fromFormat);
		SimpleDateFormat toSdf = new SimpleDateFormat(toFormat);
		try {
			return toSdf.format(formSdf.parse(obj.toString()));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return "";
	}

}
