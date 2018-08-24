package com.ftoul.bi.clean;

/**
 * 清理接口
 * @author xiaohf
 *
 */
public interface IClean {
	
	/**
	 * @param obj 对参数参数传进来的值进行清理
	 * @return 返回清理后的结果对象
	 */
	public Object clean(Object obj);
}
