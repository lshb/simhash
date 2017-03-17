package com.yqzk.markRepeat.main;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONException;

import com.alibaba.fastjson.JSONObject;
import com.lsb.util.SimHash;
import com.yq.pa_cx_data.utils.Pa_Cx_Data;
import com.yqzk.markRepeat.data.HbaseHandle;
import com.yqzk.markRepeat.data.IndexHandle;
import com.yqzk.markRepeat.data.RedisHandle;
import com.yqzk.markRepeat.dataFilter.NewsDataFilter;
import com.yqzk.markRepeat.doc.Document;

public class MarkRepeat {

	private static Logger log = Logger.getLogger(MarkRepeat.class);
	private HbaseHandle hbase = null;
	private IndexHandle index = null;
	private RedisHandle redis = null;
	private int customID;
	private int threshold;

	public MarkRepeat(int customID) {
		this.customID = customID;
		this.hbase = new HbaseHandle(customID);
		this.index = new IndexHandle(customID);
		this.redis = new RedisHandle(customID);
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
					synchronized (flag) {
						log.info("*****************" + flag
								+ "*******************");
						isRepeat = judge(doc);
					}
				} else {
					flag = "TITLE";
					threshold = 0;
					doc.generateHash(flag);
					redis.init(flag);
					synchronized (flag) {
						log.info("*****************" + flag
								+ "*******************");
						isRepeat = judge(doc);
					}
				}
				updateNewData(doc);
			}
			String content = paData.getI_CONTENT();
			if (content == null || content.length() == 0) {
			} else if (!isRepeat) {
				String flag = "CONTENT";
				threshold = 2;
				doc.generateHash(flag);
				redis.init(flag);
				synchronized (flag) {
					log
							.info("*****************" + flag
									+ "*******************");
					isRepeat |= judge(doc);
				}
				updateNewData(doc);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			log.error("判重方法work中出现异常！", e);
		}
		if (!isRepeat) {
			String docID = paData.getI_ID();
			if ("0".equals(docID)) {
				log
						.warn("%%%%%%%%%%%%%%%%%%%%%%%group不应该出现0的情况%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
			}
			updateData(paData, "1", docID, 0);
		}
		
		return isRepeat;
	}

	private boolean isRepeat = false;

	// 去重判断
	private boolean judge(Document doc) throws IOException {
		isRepeat = false;
		//140279148473433203
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
							redis.addGROUP_IDs(groupId, id);
							redis.setID_GROUP(id, groupId);
						}
						// 获得碰撞的list
						List<String> touchList = touch(docID, id, groupId);

						// 获得重复条数
						int length = touchList.size();

						if (length < 2) {
							log.error(groupId + "批次相同文章数量小于2！不应该有这种情况！length值为"+length);
						}

						String oldId = touchList.get(length - 2);
						log.info("------------------------------更新倒数第" + 2
								+ "个数据:" + oldId);
						Pa_Cx_Data pa1 = hbase.selectSingle(oldId);// 查询是为了索引数据更新
						updateData(pa1, "0", groupId, length); // 必须为groupId，第一次发生碰撞，为第一个数据

						String newId = touchList.get(length - 1);
						log.info("------------------------------更新倒数第" + 1
								+ "个数据:" + newId);
						Pa_Cx_Data pa2 = hbase.selectSingle(newId);// 查询是为了索引数据更新
						updateData(pa2, "1", groupId, length);

						if ("0".equals(groupId)) {
							log
									.warn("%%%%%%%%%%%%%%%%%%%%%%%group不应该出现0的情况%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
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
		} catch (Exception e) {
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
