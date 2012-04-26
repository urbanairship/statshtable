package com.urbanairship.statshtable;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Singleton containing the thread pool for updating the timer metrics.
 */
public class TimerThreadPoolExecutor {

    private static final TimerThreadPoolExecutor instance = new TimerThreadPoolExecutor();
    private static ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private TimerThreadPoolExecutor() {
        scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(8);
    }

    public static ScheduledThreadPoolExecutor getTimerThreadPoolExecutor() {
        return scheduledThreadPoolExecutor;
    }
}
