/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.statshtable;

/**
 * There is a singleton AtomicRegistry that contains all the TimerMetrics that track individual region latencies.
 * This class provides access to that singleton and instantiates it the first time it is called.
 */
public class RegionTimers {
    private static StatsTimerRegistry instance = null;
    private static final Object initLock = new Object();
    
    static StatsTimerRegistry getInstance() {
        if(instance != null) {
            return instance;
        }
        synchronized(initLock) {
            if(instance != null) {
                return instance;
            }
            instance = new StatsTimerRegistry("_regions");
            return instance;
        }
    }
}
