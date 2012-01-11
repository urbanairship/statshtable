package com.urbanairship.statshtable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import com.yammer.metrics.core.GaugeMetric;

/**
 * Gives a JMX gauge that returns stack traces of the slowest N queries as JSON.
 * 
 * Thread safe!
 */
public class SlowQueryGauge extends GaugeMetric<String> {
    private static final Log log = LogFactory.getLog(SlowQueryGauge.class);
    
    NavigableSet<TimedStackTrace> slowQueries = new TreeSet<TimedStackTrace>();
    private static ObjectMapper mapper = new ObjectMapper();
    private int howMany;
    
    public SlowQueryGauge(int howMany) {
        if(howMany < 1) {
            throw new IllegalArgumentException("howMany must be >= 1");
        }
        this.howMany = howMany;
    }
    
    synchronized public void maybeUpdate(long latencyNanos) {
        boolean shouldSave = false;
        
        if(slowQueries.size() < howMany) {
            shouldSave = true;
        } else {
            long smallestStoredLatency = slowQueries.first().latency;

            if(smallestStoredLatency < latencyNanos) {
                shouldSave = true;
            }
        }
        
        if(shouldSave) {
            slowQueries.add(new TimedStackTrace(Thread.currentThread().getStackTrace(), latencyNanos));
            while(slowQueries.size() > howMany) {
                slowQueries.pollFirst(); // pop the stack trace with the lowest latency
            }
        }
    }

    /**
     * Called by JMX to output the worst N stack traces, highest latency first.
     */
    @Override
    public String value() {
        try {
            Map<StackTraceElement[],Long> jsonMap = new LinkedHashMap<StackTraceElement[],Long>();
            synchronized (this) {
                for(TimedStackTrace timedStackTrace: slowQueries.descendingSet()) {
                    jsonMap.put(timedStackTrace.stackTrace, timedStackTrace.latency);
                }
            }
            return mapper.writeValueAsString(jsonMap);
        } catch (Exception e) {
            log.warn("Exception making JSON for slow queries", e);
            return "exception";
        }
    }
    
    /**
     * A struct that stores a record of a stack trace with associated latency. It's Comparable so we 
     * can keep a sorted set of the highest latencies.
     */
    private static class TimedStackTrace implements Comparable<TimedStackTrace> {
        public final StackTraceElement[] stackTrace;
        public final long latency;
        
        public TimedStackTrace(StackTraceElement[] stackTrace, long latency) {
            this.stackTrace = stackTrace;
            this.latency = latency;
        }

        @Override
        public int compareTo(TimedStackTrace other) {
            int cmp;
            
            // Comparison is based on numerical order of their latency
            cmp = (int)(this.latency - other.latency);
            if(cmp != 0) {
                return cmp;
            }
            
            // Latencies were equal, so we need a tiebreaker. It's OK if these collide sometimes.
            return this.hashCode() - other.hashCode();
        }
    }
}
