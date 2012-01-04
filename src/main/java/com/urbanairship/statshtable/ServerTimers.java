package com.urbanairship.statshtable;

/**
 * There is a singleton AtomicRegistry that contains all the TimerMetrics that track individual server latencies.
 * This class provides access to that singleton and instantiates it the first time it is called.
 */
public class ServerTimers {
    private static AtomicRegistry<String,String,SHTimerMetric> instance = null;
    private static final Object initLock = new Object();
    
    static AtomicRegistry<String,String,SHTimerMetric> getInstance() {
        if(instance != null) {
            return instance;
        }
        synchronized(initLock) {
            if(instance != null) {
                return instance;
            }
            instance = new AtomicRegistry<String,String,SHTimerMetric>();
            return instance;
        }
    }
}
