package com.yqzk.markRepeat.main;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import com.yqzk.markRepeat.conf.Config;

public class MarkMain {

	private String[] customIDs = Config.customIDs;
	private Consumer[] consumers = new Consumer[customIDs.length];
	private Thread[] threads = new Thread[customIDs.length];

	public void work() {
		for (int i = 0; i < customIDs.length; i++) {
			consumers[i] = new Consumer(customIDs[i]);
			threads[i] = new Thread(consumers[i], customIDs[i] + "客户");
			threads[i].start();
		}
		Timer timer = new Timer("去重守护线程",false);
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				for (int i = 0; i < customIDs.length; i++) {
					if (!consumers[i].isRuning()) {
						try {
							consumers[i].stop();
						} catch (IOException e) {
							e.printStackTrace();
						}
						threads[i].interrupt();
						try {
							threads[i].join();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						consumers[i] = new Consumer(customIDs[i]);
						threads[i] = new Thread(consumers[i], customIDs[i] + "客户");
						threads[i].start();
					}
				}
			}
		}, 0, 10*60*1000);
	}

	public static void main(String[] args) {
		MarkMain main = new MarkMain();
		main.work();
	}
}
