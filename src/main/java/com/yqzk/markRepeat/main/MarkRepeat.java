package com.yqzk.markRepeat.main;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.jruby.compiler.ir.compiler_pass.IR_Printer;
import org.json.JSONException;

import com.alibaba.fastjson.JSONObject;
import com.lsb.util.SimHash;
import com.yq.pa_cx_data.utils.Pa_Cx_Data;
import com.yqzk.markRepeat.data.HbaseHandle;
import com.yqzk.markRepeat.data.IndexHandle;
import com.yqzk.markRepeat.data.RabbitmqHandle;
import com.yqzk.markRepeat.data.RedisHandle;
import com.yqzk.markRepeat.dataFilter.NewsDataFilter;
import com.yqzk.markRepeat.doc.Document;

public class MarkRepeat {

	private static Logger log = Logger.getLogger(MarkRepeat.class);
	private HbaseHandle hbase = null;
	private IndexHandle index = null;
	private RedisHandle redis = null;
	private RabbitmqHandle rabbit = null;
	private int customID;
	private int threshold;

	public MarkRepeat(int customID) {
		this.customID = customID;
		this.hbase = new HbaseHandle(customID);
		this.index = new IndexHandle(customID);
		this.redis = new RedisHandle(customID);
		this.rabbit = new RabbitmqHandle(customID);
	}

	private static NewsDataFilter ndf = new NewsDataFilter();

	/**
	 * 去重
	 * 
	 * @param paData
	 * @throws ParseException
	 * @throws IOException
	 */
	/**
	 * @param paData
	 * @return
	 */
	public boolean work(Pa_Cx_Data paData) {
		if (paData == null) {
			log.error("数据为null！");
			return false;
		}
		// 对数据进行过滤，保证与索引中的一致
		JSONObject json = (JSONObject) JSONObject.toJSON(paData);
		json = ndf.doFilter(json);
		if (json == null) {
			log.info("数据不符合格式要求，被过滤掉！");
			return false;
		}
		// 已经经过去重
		if (redis.exitID_GROUP(paData.getI_ID())) {
			log.info("数据已经去重，不需要再次去重!");
			return true;
		}
		// 以上为数据过滤部分
		Document doc = new Document(paData);
		try {
			String title = paData.getI_TITLE();
			String type = paData.getI_TYPE();
			if (title == null || title.length() == 0) {
			} else {
				String flag = null;
				if ("4".equals(type)) {
					flag = "WEIBO";
					threshold = 2;
					doc.generateHash("TITLE");
					redis.init(flag);
				} else {
					flag = "TITLE";
					threshold = 0;
					doc.generateHash(flag);
					redis.init(flag);
				}
				log.info("*****************" + flag + "*******************");
				isRepeat = judge(doc);
			}
			String content = paData.getI_CONTENT();
			if (content == null || content.length() == 0) {
			} else if (!isRepeat) {
				String flag = "CONTENT";
				threshold = 2;
				doc.generateHash(flag);
				log.info("*****************" + flag + "*******************");
				redis.init(flag);
				isRepeat |= judge(doc);

			}
		} catch (IOException e) {
			e.printStackTrace();
			log.error("判重方法work中出现异常！", e);
		}
		updateNewData(doc);
		return isRepeat;
	}

	private boolean isRepeat = false;

	// 去重判断
	private boolean judge(Document doc) throws IOException {
		isRepeat = false;
		String docID = doc.getPaData().getI_ID();

		BigInteger docIntHash = doc.getSimHash().intSimHash;
		String docStrHash = doc.getSimHash().strSimHash;
		// 遍历doc每16位的hash码
		for (int i = 0; i < 4; i++) {
			log.info("hash从" + i + "位置开始进行匹配,docID=" + docID + ", hash="
					+ docIntHash);
			String hash1_4 = docStrHash.substring(i * 16, (i + 1) * 16);
			List<BigInteger> list = redis.getHashes(i, hash1_4);
			if (list.size() != 0) {
				// 遍历hash
				for (BigInteger hash : list) {
					if (SimHash.hammingDistance(hash, docIntHash) <= threshold) {
						String id = redis.getID(hash);
						log.info("发现hammingDistance距离 <= " + threshold);
						// 将id加入到docID队列中
						String groupId = redis.getID_GROUP(id);
						// 第一次发生碰撞的数据没有groupId,为空
						if (groupId == null || groupId.length() == 0) {
							groupId = id;
							redis.addGROUP_IDs(id, id);
							redis.setID_GROUP(id, id);
						}
						// 获得碰撞的list
						List<String> touchList = touch(docID, id, groupId);

						// 获得重复条数
						int length = touchList.size();

						if (length < 2) {
							log.error(groupId + "批次相同文章数量小于2！不应该有这种情况！");
						}

						// String oldId = touchList.get(length - 2);
						// log.info("------------------------------更新倒数第" + 2
						// + "个数据:" + oldId);
						// Pa_Cx_Data pa1 = hbase.selectSingle(oldId);//
						// 查询是为了索引数据更新
						// updateData(pa1, "0", null, length);
						//
						// String newId = touchList.get(length - 2);
						// log.info("------------------------------更新倒数第" + 1
						// + "个数据:" + newId);
						// Pa_Cx_Data pa2 = hbase.selectSingle(newId);//
						// 查询是为了索引数据更新
						// updateData(pa2, "1", groupId, length);

						// 下面注释部分是更新所有重复数据的num，上面只更改了最后两次发生碰撞的数据
						int num = 0;
						for (String string : touchList) {
							num++;

							if (num == length - 2) {
								log.info("------------------------------更新第"
										+ num + "个数据:" + string);
								Pa_Cx_Data pa1 = hbase.selectSingle(string);//
								// 查询是为了索引数据更新
								updateData(pa1, "0", null, length);
								continue;
							}

							if (num == length - 1) {
								log.info("------------------------------更新第"
										+ num + "个数据:" + string);
								Pa_Cx_Data pa2 = hbase.selectSingle(string);//
								// 查询是为了索引数据更新
								updateData(pa2, "1", groupId, length);
								continue;
							}
							log.info("------------------------------更新第" + num
									+ "个数据:" + string);
							// 动态更新索引数据库，
							updateDB(string, null, null, length);
							Pa_Cx_Data pa3 = hbase.selectSingle(string);
							updateIndex(pa3, null, null, length);
						}
						return true;
					}
				}
			}
		}
		return false;
	}

	private void updateNewData(Document doc) {
		// 完善pa返回数据
		Pa_Cx_Data pa = doc.getPaData();
		String docID = pa.getI_ID();
		BigInteger intHash = doc.getSimHash().intSimHash;
		String strHash = doc.getSimHash().strSimHash;
		if (!isRepeat) {
			updateData(pa, null, docID, 0);
		}
		redis.addHashs(docID, intHash, strHash);
		redis.addIdHashAndHashId(docID, intHash);
	}

	private List<String> touch(String docID, String id, String groupId) {
		List<String> list = redis.addGROUP_IDs(groupId, docID);
		redis.setID_GROUP(docID, groupId);
		return list;
	}

	// 更新数据库及索引
	private void updateData(Pa_Cx_Data pa, String show, String repeatGroup,
			Integer nums) {
		updateDB(pa.getI_ID(), show, repeatGroup, nums);
		updateIndex(pa, show, repeatGroup, nums);
	}

	private void updateIndex(Pa_Cx_Data pa, String show, String repeatGroup,
			Integer nums) {
		boolean flag = false;
		// 更新索引
		if (show != null) {
			pa.setI_SHOW(show);
			flag = true;
		}
		if (repeatGroup != null) {
			pa.setI_REPEATGROUP(repeatGroup);
			flag = true;
		}
		if (nums != null) {
			pa.setI_REPEATNUM(String.valueOf(nums));
			flag = true;
		}
		try {
			if (flag) {
				index.update(pa);
			}
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void updateDB(String id, String show, String repeatGroup,
			Integer nums) {
		// 更新hbase数据库
		try {
			if (show != null) {
				hbase.updateShow(id, show);
			}
			if (repeatGroup != null) {
				hbase.updateRepeatNum(id, String.valueOf(nums));
			}
			if (nums != null) {
				hbase.updateRepeatGroup(id, repeatGroup);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		if (hbase != null) {
			hbase.close();
			hbase = null;
		}
		if (redis != null) {
			redis.close();
			redis = null;
		}
		if (index != null) {
			index.close();
			index = null;
		}
		// if (rabbit != null) {
		// rabbit.close();
		// rabbit = null;
		// }
	}

	public int getCustomID() {
		return customID;
	}
}
