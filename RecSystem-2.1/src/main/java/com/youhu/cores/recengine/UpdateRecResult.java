package com.youhu.cores.recengine;

import java.util.Timer;

public class UpdateRecResult {
	public static void timeMaker() {
		Timer timer = new Timer();
		/** 在1min后执行此任务,每次间隔1min,再次执行一次 */
		timer.schedule(new UpdateModelThread(), 3 * 60 * 1000, 1 * 60 * 1000);
	}
}
