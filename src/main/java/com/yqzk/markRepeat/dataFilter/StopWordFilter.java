package com.yqzk.markRepeat.dataFilter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;

import org.ansj.domain.Term;
import org.ansj.util.FilterModifWord;
import org.apache.log4j.Logger;

public class StopWordFilter {
	private static Logger log = Logger.getLogger(StopWordFilter.class);
	private static HashMap<String, String> stopWords = new HashMap<String, String>();

	static {
		InputStream in = StopWordFilter.class.getClassLoader().getResourceAsStream("stopWord.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		try {
			while ((line=br.readLine())!=null) {
				if (line.startsWith("#")) {
					continue;
				}
				if (line.trim().length()==0) {
					continue;
				}
//				log.info(line.trim());
				stopWords.put(line.trim(), FilterModifWord._stop);
			}
		} catch (IOException e) {
			log.error("读取停用词文件时报错！",e);
		}
		FilterModifWord.setUpdateDic(stopWords);
		log.info("****************************装载停用词！********************************");
	}

	public static List<Term> doFilter(List<Term> terms) {
		return  FilterModifWord.modifResult(terms);
	}
	public static void main(String[] args) {
		
	}
}
