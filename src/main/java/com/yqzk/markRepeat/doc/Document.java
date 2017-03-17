package com.yqzk.markRepeat.doc;

import java.io.IOException;

import org.apache.log4j.Logger;
import com.yq.pa_cx_data.utils.Pa_Cx_Data;

public class Document {
	private static Logger log = Logger.getLogger(Document.class);
	private Pa_Cx_Data paData;
	private SimHash simHash;

	public Document(Pa_Cx_Data paData) {
		this.paData = paData;
	}

	public void generateHash(String flag) {
		String content = "";
		if ("TITLE".equals(flag)) {
			content = paData.getI_TITLE() ;
		}
		if ("CONTENT".equals(flag)) {
			content = paData.getI_CONTENT();
		}
		if (content ==null||content.length()==0) {
			log.info("hash的内容为空！");
			return;
		}
		try {
			this.simHash = new SimHash(content);
//			log.info("hash content:"+content);
		} catch (IOException e) {
			e.printStackTrace();
			log.error(e.getMessage());
		}
	}

	public SimHash getSimHash() {
		return simHash;
	}
	

	public void setSimHash(SimHash simHash) {
		this.simHash = simHash;
	}

	public Pa_Cx_Data getPaData() {
		return paData;
	}

	public void setPaData(Pa_Cx_Data paData) {
		this.paData = paData;
	}

}
