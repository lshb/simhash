package com.yqzk.markRepeat.data;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.hbase.client.ResultScanner;

import com.lsb.util.DateUtil;
import com.yq.pa_cx_data.dao.Pa_Cx_Data_DaoIm;
import com.yq.pa_cx_data.utils.Pa_Cx_Data;
import com.yqzk.markRepeat.conf.Config;

public class HbaseHandle {

	private int customID;
	private Pa_Cx_Data_DaoIm paDao = null;
	private final static String SHOW ="I_SHOW";
	private final static String REPEATNUM ="I_REPEATNUM";
	private final static String REPEATGROUP ="I_REPEATGROUP";
	
	public HbaseHandle(int customID) {
		this.customID = customID;
		this.paDao = new Pa_Cx_Data_DaoIm();
	}

	public void update(String id,String key,String value) throws IOException, ParseException {
		paDao.updateSingle(customID, id, key, value);
	}
	public void updateShow(String id,String value) throws IOException, ParseException {
		paDao.updateSingle(customID, id, SHOW, value);
	}
	public void updateRepeatNum(String id,String value) throws IOException, ParseException {
		paDao.updateSingle(customID, id, REPEATNUM, value);
	}
	public void updateRepeatGroup(String id,String value) throws IOException, ParseException {
		paDao.updateSingle(customID, id, REPEATGROUP, value);
	}
	
	public ResultScanner selectAll() throws IOException{
		return paDao.selectAll(customID, Config.indexColumns);
	}
	
	public ResultScanner selectTime(int days) throws IOException{
		long startTime = DateUtil.beforeDay(days).getTime();
		long stopTime = System.currentTimeMillis();
		return paDao.selectTime(customID,startTime, stopTime, Config.indexColumns);
	}
	
	public Pa_Cx_Data selectSingle(String id) throws IOException{
		return paDao.selectSingle(customID, id);
	}
	
	
	public void close(){
		if (paDao!=null) {
//			paDao.close();	
			paDao = null;
		}
	}
}
