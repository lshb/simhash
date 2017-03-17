package com.yqzk.markRepeat.doc;
/**
 * Function: simHash 判断文本相似度，该示例程支持中文<br/>
 * date: 2013-8-6 上午1:11:48 <br/>
 * @author june
 * @version 0.1
 */
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

import com.yqzk.markRepeat.dataFilter.StopWordFilter;

public class SimHash {

	public BigInteger intSimHash;

	public String strSimHash;

	public int hashbits = 64;

	public SimHash(String text) throws IOException {
		this.intSimHash = this.simHash(text);
	}

	public SimHash(String tokens, int hashbits) throws IOException {
		this.hashbits = hashbits;
		this.intSimHash = this.simHash(tokens);
	}

//	HashMap<String, Integer> wordMap = new HashMap<String, Integer>();

	public BigInteger simHash(String text) throws IOException {
		// 定义特征向量/数组
		int[] v = new int[this.hashbits];
		// 英文分词
		// StringTokenizer stringTokens = new StringTokenizer(this.tokens);
		// while (stringTokens.hasMoreTokens()) {
		// String temp = stringTokens.nextToken();
		// }
		//ansj分词
		List<Term> terms = ToAnalysis.parse(text);
		terms = StopWordFilter.doFilter(terms);
		Map<String, Integer> wordMap = new HashMap<String, Integer>();
		Iterator<Term> it = terms.iterator();
		while (it.hasNext()) {
			Term te = (Term) it.next();
			String word = te.getName();
//			去掉空字符串
			if (word.trim().length()==0) {
				continue;
			}
			//去除长度为1的词汇
//			if (word.length()==1) {
//				continue;
//			}
			if (wordMap.containsKey(word)) {
				wordMap.put(te.getName(), wordMap.get(word)+1);
			}else {
				wordMap.put(te.getName(), 1);
			}
		}
		for (String word:wordMap.keySet()) {
//			System.err.print(word+":"+wordMap.get(word)+",");
			// 注意停用词会被干掉
			// 2、将每一个分词hash为一组固定长度的数列.比如 64bit 的一个整数.
			BigInteger t = this.hash(word);
//			System.err.print(word+",");
			for (int i = 0; i < this.hashbits; i++) {
				BigInteger bitmask = new BigInteger("1").shiftLeft(i);
				// 3、建立一个长度为64的整数数组(假设要生成64位的数字指纹,也可以是其它数字),
				// 对每一个分词hash后的数列进行判断,如果是1000...1,那么数组的第一位和末尾一位加1,
				// 中间的62位减一,也就是说,逢1加1,逢0减1.一直到把所有的分词hash数列全部判断完毕.
				if (t.and(bitmask).signum() != 0) {
					// 这里是计算整个文档的所有特征的向量和
					// 这里实际使用中需要 +- 权重，比如词频，而不是简单的 +1/-1，
					v[i] += wordMap.get(word);
				} else {
					v[i] -= wordMap.get(word);
				}
			}
		}

		BigInteger fingerprint = new BigInteger("0");
		StringBuffer simHashBuffer = new StringBuffer();
		for (int i = 0; i < this.hashbits; i++) {
			// 4、最后对数组进行判断,大于0的记为1,小于等于0的记为0,得到一个 64bit 的数字指纹/签名.
			if (v[i] >= 0) {
				fingerprint = fingerprint.add(new BigInteger("1").shiftLeft(i));
				simHashBuffer.append("1");
			} else {
				simHashBuffer.append("0");
			}
		}
		this.strSimHash = simHashBuffer.toString();
//		System.out.println(this.strSimHash + " length " + this.strSimHash.length());
		return fingerprint;
	}

	private BigInteger hash(String source) {
		if (source == null || source.length() == 0) {
			return new BigInteger("0");
		} else {
			char[] sourceArray = source.toCharArray();
			BigInteger x = BigInteger.valueOf(((long) sourceArray[0]) << 7);
			BigInteger m = new BigInteger("1000003");
			BigInteger mask = new BigInteger("2").pow(this.hashbits).subtract(new BigInteger("1"));
			for (char item : sourceArray) {
				BigInteger temp = BigInteger.valueOf((long) item);
				x = x.multiply(m).xor(temp).and(mask);
			}
			x = x.xor(new BigInteger(String.valueOf(source.length())));
			if (x.equals(new BigInteger("-1"))) {
				x = new BigInteger("-2");
			}
			return x;
		}
	}

	public static int hammingDistance(BigInteger one,BigInteger other) {

		BigInteger x = one.xor(other);
		int tot = 0;

		// 统计x中二进制位数为1的个数
		// 我们想想，一个二进制数减去1，那么，从最后那个1（包括那个1）后面的数字全都反了，
		// 对吧，然后，n&(n-1)就相当于把后面的数字清0，
		// 我们看n能做多少次这样的操作就OK了。

		while (x.signum() != 0) {
			tot += 1;
			x = x.and(x.subtract(new BigInteger("1")));
		}
//		System.err.println(tot);
		return tot;
	}
	public int hammingDistance(SimHash other) {
		
		BigInteger x = this.intSimHash.xor(other.intSimHash);
		int tot = 0;
		
		// 统计x中二进制位数为1的个数
		// 我们想想，一个二进制数减去1，那么，从最后那个1（包括那个1）后面的数字全都反了，
		// 对吧，然后，n&(n-1)就相当于把后面的数字清0，
		// 我们看n能做多少次这样的操作就OK了。
		
		while (x.signum() != 0) {
			tot += 1;
			x = x.and(x.subtract(new BigInteger("1")));
		}
//		System.err.println(tot);
		return tot;
	}

	public int getDistance(String str1, String str2) {
		int distance;
		if (str1.length() != str2.length()) {
			distance = -1;
		} else {
			distance = 0;
			for (int i = 0; i < str1.length(); i++) {
				if (str1.charAt(i) != str2.charAt(i)) {
					distance++;
				}
			}
		}
		return distance;
	}

	public List subByDistance(SimHash simHash, int distance) {
		// 分成几组来检查
		int numEach = this.hashbits / (distance + 1);
		List characters = new ArrayList();

		StringBuffer buffer = new StringBuffer();

		int k = 0;
		for (int i = 0; i < this.intSimHash.bitLength(); i++) {
			// 当且仅当设置了指定的位时，返回 true
			boolean sr = simHash.intSimHash.testBit(i);

			if (sr) {
				buffer.append("1");
			} else {
				buffer.append("0");
			}

			if ((i + 1) % numEach == 0) {
				// 将二进制转为BigInteger
				BigInteger eachValue = new BigInteger(buffer.toString(), 2);
				System.out.println("----" + eachValue);
				buffer.delete(0, buffer.length());
				characters.add(eachValue);
			}
		}

		return characters;
	}

	public static void main(String[] args) throws IOException {
//		ToAnalysis.parse("");
//		String text = "昨天，市食药监局根据食品安全风险评估结果，对6种不合格产品下达了全市统一下架指令。 检出霉菌超标的有两款食品，均为北京康兴达食品有限公司生产的艾窝窝;北京市天瑞祥食品加工中心生产的一款酱猪耳，检出金黄色葡萄球菌;北京百世醇香工贸有限公司生产的一款蒜味肉肠、一款蒜蓉烧肠，均检出金黄色葡萄球菌;北京口口福肉食加工厂生产的一款“金盈”柴母鸡，检出沙门氏菌。 责任编辑：小美";
//		SimHash simHash = new SimHash(text);
//		System.err.println();
//		String text1 = "食品伙伴网讯据欧盟食品安全局（EFSA）消息，1月21日欧盟食品安全局就修订谷物和动物源食物中矮壮素（chlormequat）的最大残留限量发布了意见。 依据欧盟委员会法规（EC）No396/2005第6章的规定，英国收到BASF公司的申请，要求修订环矮壮素在谷物和多种动物源性食品中的MRL。同时受到荷兰一家公司的申请，要求将梨中矮壮素的最大残留限量保持为0.1mg/kg。 根据风险评估结果，欧盟食品安全局认为矮壮素最大残留限量的修订不会导致消费者接触超出毒理学参考值，因此不会危害公众健康。 欧盟食品安全局对评估报告进行评审后，建议矮壮素的最大残留限量具体修订信息如下： 原文链接：";
//		SimHash simHash1 = new SimHash(text1);
//		System.err.println(simHash.intSimHash);
//		System.err.println(simHash.strSimHash);
//		System.err.println(simHash1.intSimHash);
//		System.err.println(simHash1.strSimHash);
//		System.err.println(simHash.getDistance(simHash.strSimHash, simHash1.strSimHash));
	}
}

