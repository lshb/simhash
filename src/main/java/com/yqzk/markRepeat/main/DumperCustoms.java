package com.yqzk.markRepeat.main;

import org.apache.log4j.Logger;

import com.yqzk.markRepeat.conf.Config;

public class DumperCustoms {
	private static Logger log = Logger.getLogger(DumperCustoms.class);

	public static void main(String[] args) {
		if (args != null && args.length != 0) {
			init(args);
		}
		String[] customs = Config.customIDs;
		for (int i = 0; i < customs.length; i++) {
			Dumper dum = new Dumper(Integer.valueOf(customs[i]));
			Thread t = new Thread(dum, customs[i] + "客户");
			t.start();
			log.info(customs[i] + "号客户去重程序开始运行！");
		}
	}

	public static void init(String[] args) {
		//-d 时间间隔； -c 客户(逗号为分隔符)； -m 查库模式
		for (int i = 0; i < args.length; i++) {
			if ("-d".equalsIgnoreCase(args[i])) {
				Config.days = Integer.valueOf(args[i+1]);
			}
			if ("-c".equalsIgnoreCase(args[i])) {
				Config.customIDs = args[i+1].split(",");
			}
			if ("-m".equalsIgnoreCase(args[i])) {
				Config.mode = Integer.valueOf(args[i+1]);
			}
		}
	}
}
