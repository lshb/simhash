package com.yqzk.markRepeat.data;

import java.lang.reflect.InvocationTargetException;

import org.json.JSONException;
import com.yq.index.client.IndexClient;
import com.yq.pa_cx_data.utils.Pa_Cx_Data;

public class IndexHandle {

	private int customID;
	private IndexClient ic = null;

	public IndexHandle(int customID) {
		this.customID = customID;
		this.ic = new IndexClient(customID);
	}

	public void update(Pa_Cx_Data pcd) throws NoSuchMethodException,
			InvocationTargetException, IllegalAccessException, JSONException {
		ic.updateNews(pcd);
	}

	public int getCustomID() {
		return customID;
	}

	public void close() {
		ic.shutdown();
	}
}
