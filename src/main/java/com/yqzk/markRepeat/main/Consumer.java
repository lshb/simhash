package com.yqzk.markRepeat.main;

import java.io.IOException;

import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;

import com.yq.pa_cx_data.utils.Pa_Cx_Data;
import com.yq.rabbitmq.client.QueueConsumer;
import com.yqzk.markRepeat.conf.Config;

public class Consumer extends QueueConsumer {

	private static Logger log = Logger.getLogger(Consumer.class);
	private MarkRepeat dw = null;
	private String customID;

	public Consumer(String customID) {
		super(Config.flag, customID);
		dw = new MarkRepeat(Integer.valueOf(customID));
		log.info("初始化"+customID+"号消费者！");
	}

	@Override
	public void handleDelivery(byte[] body) {
		Pa_Cx_Data pa = (Pa_Cx_Data) SerializationUtils.deserialize(body);
		dw.work(pa);
	};

	public void stop() throws IOException {
		if (channel != null || channel.isOpen()) {
			channel.close();
		}
		if (connection != null || connection.isOpen()) {
			connection.close();
		}
		if (dw!=null) {
			close();
		}
		isRuning = false;
	}

	@Override
	public void close() {
		if (dw != null) {
			dw.close();
			dw = null;
		}
	}

}
