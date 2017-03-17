package com.yqzk.markRepeat.data;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.yq.pa_cx_data.utils.Pa_Cx_Data;
import com.yq.rabbitmq.client.Producer;
import com.yqzk.markRepeat.conf.Config;

public class RabbitmqHandle {

	private Logger log = Logger.getLogger(RabbitmqHandle.class);
	private int customID;
	private Producer producer = null;
	
	public RabbitmqHandle(int customID) {
		this.customID = customID;
		try {
			producer = new Producer(Config.flag,customID);
		} catch (IOException e) {
			log.error("初始化去重队列的producer失败",e);
		}
	}

	public void sendMessage(Pa_Cx_Data pcd){
		try {
			producer.sendMessage(pcd);
		} catch (IOException e) {
			e.printStackTrace();
			log.error("发送去重队列消息失败！"+pcd,e);
		}
	}
	
	public int getCustomID() {
		return customID;
	}

//	public void close(){
//		if (producer!=null) {
//			try {
//				producer.close();
//			} catch (IOException e) {
//				log.error("关闭去重producer失败！",e);
//			}
//			producer=null;
//		}
//	}
}
