package com.yqzk.markRepeat.data;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.lsb.util.ObjectAndByteArrayConvertUtil;
import com.yq.redis.RedisClient;

public class RedisHandle {

	private static Logger log = Logger.getLogger(RedisHandle.class);
	private RedisClient redisClient = new RedisClient(); 
	private int customID; //客户id
	private String ID_GROUP = null;  //保存id-group的关系
	private String GROUP_IDS = null;  //保存group-id的关系
	private String HASHS = null; //保存hashs
	private String ID_HASH = null; //保存id-hash的关系
	private String HASH_ID = null; //保存hash-id的关系
	private String FLAG = null; //标题和内容标识
	
	public RedisHandle(int customID) {
		this.customID = customID;
		this.ID_GROUP = customID+"ID_GROUP";
		this.GROUP_IDS = customID+"GROUP_IDS";
	}
	
	public void init(String flag){
		this.FLAG = flag;
		this.HASHS = customID+flag+"HASHS";
		this.ID_HASH = customID+flag+"ID_HASH";
		this.HASH_ID = customID+flag+"HASH_ID";
	}

	public List<BigInteger> getHashes(int num,String key){
		byte[] HASHSBytes =(HASHS+num).getBytes();
		byte[] keyBytes = key.getBytes();

		if (!redisClient.hexists(HASHSBytes, keyBytes)) {
			return new ArrayList<BigInteger>();
		} else {
			byte[] hashsBytes = redisClient.hget(HASHSBytes, keyBytes);
			List<BigInteger> hashs = (List<BigInteger>) ObjectAndByteArrayConvertUtil
					.ByteArrayToObject(hashsBytes);
			return hashs;
		}
	}
	
	public void setHashes(int num,String key,List<BigInteger> list){
		redisClient.hset((HASHS+num).getBytes(), key.getBytes(), ObjectAndByteArrayConvertUtil.ObjectToByteArray(list));
	}
	
	public void addHashs(String docID, BigInteger docIntHash,
			String docStrHash) {
		for (int i = 0; i < 4; i++) {
			String hash1_4 = docStrHash.substring(i * 16, (i + 1) * 16);

			byte[] hashsList = redisClient.hget((HASHS + i).getBytes(), hash1_4
					.getBytes());
			List<BigInteger> list = null;
			if (hashsList != null) {
				list = (List<BigInteger>) ObjectAndByteArrayConvertUtil
						.ByteArrayToObject(hashsList);
			} else {
				list = new ArrayList<BigInteger>();
			}
			list.add(docIntHash);
			byte[] listList = ObjectAndByteArrayConvertUtil
					.ObjectToByteArray(list);
			redisClient.hset((HASHS + i).getBytes(), hash1_4.getBytes(),
					listList);
		}
		log.info("添加"+docID+"的hash到"+FLAG+"中！");
	}
	/**
	 * 将id添加到group-id的关系中
	 * 
	 * @param groupID
	 * @param id
	 * @return  groupIDlist中的数量
	 */
	public List<String> addGROUP_IDs(String groupID, String id) {
		if (groupID==null) {
			log.info("groupID为空！");
		}
		List<String> list = getGROUP_IDSList(groupID);
		if (list.contains(id)) {
			log.info(groupID+"的GROUP_IDs中已经存在"+id);
			return list;
		}else {
			list.add(id);
		}
		setGROUP_IDSList(groupID,list);
		return list;
	}
	/**
	 * 设置id—group关系
	 * @param id
	 * @param groupID
	 */
	public void setID_GROUP(String id,String groupID) {
		if (redisClient.hexists(ID_GROUP, id)) {
			log.warn("已经存在"+id+"-"+groupID+"的关系！");
		}
		redisClient.hset(ID_GROUP, id, groupID);
	}
	/**
	 * 判断是否存在id—group关系
	 * @param id
	 * @return 
	 */
	public boolean exitID_GROUP(String id) {
		return redisClient.hexists(ID_GROUP, id);
	}
	/**
	 * 获得id对应的group
	 * @param id
	 * @param groupID
	 */
	public String getID_GROUP(String id) {
		return redisClient.hget(ID_GROUP, id);
	}

	public void setGROUP_IDSList(String key,  List<String> list) {
		redisClient.hset(GROUP_IDS.getBytes(), key.getBytes(), ObjectAndByteArrayConvertUtil.ObjectToByteArray(list));
	}

	public List<String> getGROUP_IDSList(String key) {
		byte[] GROUP_IDSBytes = GROUP_IDS.getBytes();
		byte[] keyBytes = key.getBytes();

		if (!redisClient.hexists(GROUP_IDSBytes, keyBytes)) {
			return new ArrayList<String>();
		} else {
			byte[] idsBytes = redisClient.hget(GROUP_IDSBytes, keyBytes);
			List<String> ids = (List<String>) ObjectAndByteArrayConvertUtil
					.ByteArrayToObject(idsBytes);
			return ids;
		}
	}
	//通过id获取hash
	public String getID(BigInteger key) {
		String keyStr = key.toString();
		if (!redisClient.hexists(HASH_ID, keyStr)) {
			return "";
		} else {
			String value = redisClient.hget(HASH_ID, keyStr);
			return value;
		}
	}
	//通过hash来获取id
//	public BigInteger getHASH(String key) {
//		if (!redisClient.hexists(ID_HASH, key)) {
//			return null;
//		} else {
//			BigInteger value =BigInteger.valueOf(Long.parseLong(redisClient.hget(ID_HASH, key)));
//			return value;
//		}
//	}
	
	public void addIdHashAndHashId(String docID,BigInteger docIntHash){
		redisClient.hset(ID_HASH, docID, docIntHash.toString());
		redisClient.hset(HASH_ID, docIntHash.toString(), docID);
	}

	public int getCustomID() {
		return customID;
	}

	public void close() {
		redisClient.close();
	}
	
	public String getFLAG() {
		return FLAG;
	}

	public void setFLAG(String fLAG) {
		FLAG = fLAG;
	}
	public static void main(String[] args) {
		//139020648445790403
		RedisHandle rh = new RedisHandle(1);
//		rh.init("TITLE");
//		rh.init("CONTENT");
		//不管哪个模式，groupid不变的
		List<String> ids = rh.getGROUP_IDSList("139021326411511503");
		int num = 0 ;
		for (Iterator iterator = ids.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.err.println(string);
			num++;
		}
		System.err.println("总数："+num);
	}
}
