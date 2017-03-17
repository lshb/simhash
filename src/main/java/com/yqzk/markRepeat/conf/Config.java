package com.yqzk.markRepeat.conf;

import com.lsb.util.ConfigManager;

public class Config {
	
	public final static String flag = "delWeight";
//	public static String[] customsID = {"1","38","53","54"};
	public static String[] customIDs = ConfigManager.instance().getProperty("customs", "customIDs").split(",");
	
	//columns必须与索引中存储字段相同
	public static String[] indexColumns = {"I_TYPE", "I_PUBTIME",
		"I_URL", "I_TITLE", "I_SOURCE", "I_CONTENT" ,"I_AREA","I_TOPIC","I_ABSTRACT","I_REPEATNUM","I_SHOW","I_REPEATGROUP","I_FEEL","I_MEDIA"};
	//mode==0：部分查询；mode==1：全部查询
	public static int mode = 0;
	//查询hbase的时间间隔days
	public static int days = 2;
	
	public final static int FEELRADIUS = 100;
	
}