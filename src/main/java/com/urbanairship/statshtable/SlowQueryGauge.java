package com.urbanairship.statshtable;

import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.yammer.metrics.core.GaugeMetric;

/**
 * Gives a JMX gauge that returns stack traces of the slowest N queries as JSON.
 */
public class SlowQueryGauge implements GaugeMetric<String> {
    private static final Logger log = LogManager.getLogger(SlowQueryGauge.class);
    
    NavigableMap<Long,StackTraceElement[]> slowQueries = new TreeMap<Long,StackTraceElement[]>();
    private static ObjectMapper mapper = new ObjectMapper();
    private int howMany;
    
    public SlowQueryGauge(int howMany) {
        if(howMany < 1) {
            throw new IllegalArgumentException("howMany must be >= 1");
        }
        this.howMany = howMany;
    }
    
    synchronized public void maybeUpdate(long latencyMs) {
        boolean shouldSave = false;
        
        if(slowQueries.size() < howMany) {
            shouldSave = true;
        } else {
            long smallestStoredLatency = slowQueries.firstKey();

            if(smallestStoredLatency < latencyMs) {
                shouldSave = true;
            }
        }
        
        if(shouldSave) {
            slowQueries.put(latencyMs, Thread.currentThread().getStackTrace());
            if(slowQueries.size() > howMany) {
                slowQueries.remove(slowQueries.firstKey());
            }
        }
    }

    @Override
    public String value() {
        try {
            synchronized (this) {
                return mapper.writeValueAsString(slowQueries.descendingMap());
            }
        } catch (Exception e) {
            log.warn("Exception making JSON for slow queries", e);
            return "exception";
        }
    }
}
