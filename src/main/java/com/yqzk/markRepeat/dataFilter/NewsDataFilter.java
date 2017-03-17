package com.yqzk.markRepeat.dataFilter;

import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import com.alibaba.fastjson.JSONObject;

import com.yqzk.markRepeat.conf.Config;

public class NewsDataFilter {
	protected Logger log = Logger.getLogger(NewsDataFilter.class);

	public JSONObject doFilter(JSONObject json) {
		long id = 0;
		if (json == null) {
			return null;
		}
		try {
			if (json.containsKey("type")) {
				String indexType = json.getString("type");
				if ("delete".equals(indexType)) {
					return json;
				}
			}
			// <column name="I_ID" type="long" />必须字段
			System.err.println(json.getString("i_ID"));
			id = Long.parseLong(json.getString("i_ID"));
			json.put("i_ID", id);
			// <column name="i_TYPE" type="int" />
			// i_TYPE默认值0
			String i_TYPE = json.getString("i_TYPE");
			if (i_TYPE == null || i_TYPE.isEmpty() || "".equals(i_TYPE)) {
				json.put("i_TYPE", 0);
			} else {
				int type = Integer.parseInt(i_TYPE);
				json.put("i_TYPE", type);
			}
			// <column name="i_PUBTIME" type="long"/>
			// i_PUBTIME格式2010-10-10 10:10:10，格式不对归0
			String i_PUBTIME = json.getString("i_PUBTIME");
			Pattern p = Pattern
					.compile("\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d");
			Matcher m = p.matcher(i_PUBTIME);
			if (m.find()) {
				long time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(
						i_PUBTIME).getTime();
				json.put("i_PUBTIME", time);
			} else {
				json.put("i_PUBTIME", 0L);
			}
			// <column name="i_AREA" type="int" />
			// i_AREA默认值0 为0时
			if (!json.containsKey("i_AREA")) {
				json.put("i_AREA", 0);
			} else {
				String i_AREA = json.getString("i_AREA");
				if (i_AREA == null || i_AREA.isEmpty() || "".equals(i_AREA)) {
					json.put("i_AREA", 0);
				} else {
					int area = Integer.parseInt(i_AREA);
					json.put("i_AREA", area);
				}
			}
			// <column name="i_TOPIC" type="int" />
			// i_TOPIC默认值0
			String i_TOPIC = json.getString("i_TOPIC");
			if (i_TOPIC == null || i_TOPIC.isEmpty() || "".equals(i_TOPIC)) {
				json.put("i_TOPIC", 0);
			} else {
				int topic = Integer.parseInt(i_TOPIC);
				json.put("i_TOPIC", topic);
			}
			// <column name="i_REPEATNUM" type="int" />
			// i_REPEATNUM默认值0
			String i_REPEATNUM = json.getString("i_REPEATNUM");
			if (i_REPEATNUM == null || i_REPEATNUM.isEmpty()
					|| "".equals(i_REPEATNUM)) {
				json.put("i_REPEATNUM", 0);
			} else {
				int num = Integer.parseInt(i_REPEATNUM);
				json.put("i_REPEATNUM", num);
			}
			// <column name="i_REPEATGROUP" type="long" />
			// i_REPEATGROUP默认值0
			String i_REPEATGROUP = json.getString("i_REPEATGROUP");
			if (i_REPEATGROUP == null || i_REPEATGROUP.isEmpty()
					|| "".equals(i_REPEATGROUP)) {
				json.put("i_REPEATGROUP", 0L);
			} else {
				long repeatGroup = Long.parseLong(i_REPEATGROUP);
				json.put("i_REPEATGROUP", repeatGroup);
			}
			// <column name="i_SHOW" type="int" />
			// i_SHOW默认值1
			String i_SHOW = json.getString("i_SHOW");
			if (i_SHOW == null || i_SHOW.isEmpty() || "".equals(i_SHOW)) {
				json.put("i_SHOW", 1);
			} else {
				int show = Integer.parseInt(i_SHOW);
				json.put("i_SHOW", show);
			}
			// <column name="i_FEEL" type="int" />
			// i_FEEL默认值0
			String i_FEEL = json.getString("i_FEEL");
			if (i_FEEL == null || i_FEEL.isEmpty() || "".equals(i_FEEL)) {
				json.put("i_FEEL", trunFeel(0));
			} else {
				int feel = Integer.valueOf(i_FEEL);
				json.put("i_FEEL", trunFeel(feel));
			}
			log.info("[datafilter data] data ok, id is " + json.getLong("i_ID"));
			return json;
		} catch (Exception e) {
			log.error("[datafilter data] occur a exception,data is " + json, e);
			return null;
		}
	}

	private int trunFeel(int m) {
		return m + Config.FEELRADIUS;
	}

	public static void main(String[] args) {
		// NewsDataFilter dataFilter = new NewsDataFilter();
		// JSONObject json = new JSONObject(
		// "{'i_TOPIC':'0','i_REPEATNUM':0,'i_URL':'http://weibo.com/n/EadburtJ?c=spr_qdhz_bd_baidusmt_weibo_s','i_PUBTIME':'2013-11-22 08:26:01','i_ID':'138508204524849503','i_ABSTRACT':'','i_TYPE':'4','i_TITLE':'【时空裂痕地图系统分析:设计源于二战军事地图】盛大游戏代理运营的动态概念MMORPG《时空裂痕》将于12月12日开启内测,而据官方爆料,该作地图系统设计源自于二战军事地图。','i_AREA':'0','i_SOURCE':'新浪微博','i_SHOW':1,'i_CONTENT':'','i_REPEATGROUP':0}");
		// System.err.println(json.toString());
		// try {
		// System.out.println(dataFilter.doFilter(json.toString().getBytes()));
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// long time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(
		// "2013-11-11 09:05:32").getTime();
		// System.err.println(time);
		// JSONObject json = new JSONObject();
		// try {
		// json.put("tdype", "delete");
		//		
		// json.put("id", "546668764");
		// if (json.isNull("type")) {
		// System.err.println("sdfadfadfadfasd");
		// }
		// String indexType = json.getString("type");
		// if ("delete".equals(indexType)) {
		// System.err.println("shanchu!");
		// }
		// if (indexType==null) {
		// System.err.println("null");
		// }
		// } catch (JSONException e) {
		// e.printStackTrace();
		// System.err.println("error");
		// }
	}
}
