package com.yqzk.markRepeat.main;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.log4j.Logger;

import com.yq.pa_cx_data.utils.PaCxDat_JudjeUtils;
import com.yq.pa_cx_data.utils.Pa_Cx_Data;
import com.yqzk.markRepeat.conf.Config;
import com.yqzk.markRepeat.data.HbaseHandle;
import com.yqzk.markRepeat.data.RabbitmqHandle;

public class Dumper implements Runnable {

	private static Logger log = Logger.getLogger(Dumper.class);
	private HbaseHandle hbase = null;
	private RabbitmqHandle rabbit = null;
	private int customID;

	public Dumper(int customID) {
		this.customID = customID;
		this.hbase = new HbaseHandle(customID);
		this.rabbit = new RabbitmqHandle(customID);
	}

	public void run() {
		ResultScanner rs = null;
		try {
			if (Config.days==0) {
				rs = hbase.selectAll();	
			}else {
				rs = hbase.selectTime(Config.days);
			}
		} catch (IOException e) {
			log.error("scan hbase error!", e);
		}
		long num = 0;
		long n = 0;
		PaCxDat_JudjeUtils pj = PaCxDat_JudjeUtils.getInstance();
		for (Result result : rs) {
			Pa_Cx_Data pa = new Pa_Cx_Data();
			num++;
			boolean show = false;
			//mode==0表示部分查询
			if (Config.mode == 0) {
				show = result.containsColumn("A".getBytes(),
						"I_REPEATGROUP".getBytes());
			}
			if (show) {
				continue;
			}
			pa.setI_ID(new String(result.getRow()));
			pj.setobject(pa, result);
			rabbit.sendMessage(pa);
		}
		if (rs != null) {
			rs.close();
			log.info("客户" + customID + "hbase resultScan close!");
		}
		if (hbase != null) {
			close();
		}
		log.info("客户" + customID + "查出" + num + "条数据，其中处理重复数据" + n + "条");
	}

	public void close() {
		if (hbase != null) {
			hbase.close();
			hbase = null;
		}
//		if (rabbit != null) {
//			rabbit.close();
//			rabbit = null;
//		}
	}
}
