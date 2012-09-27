/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.statshtable;

import com.yammer.metrics.core.Gauge;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.StringWriter;
import java.util.*;


/**
 * Gives a JMX gauge that returns stack traces of the slowest N queries as JSON.
 * 
 * Thread safe!
 */
public class SlowQueryGauge extends Gauge<List<String>> {
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
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            slowQueries.add(new TimedStackTrace(stackTrace, latencyNanos));
            while(slowQueries.size() > howMany) {
                slowQueries.pollFirst(); // pop the stack trace with the lowest latency
            }
        }
    }

    /**
     * Called by JMX to output the worst N stack traces, highest latency first.
     */
    @Override
    public List<String> value() {
        try {
            List<String> jsonMap = new LinkedList<String>();
            synchronized (this) {
                for(TimedStackTrace timedStackTrace: slowQueries.descendingSet()) {
                    jsonMap.add(timedStackTrace.latency + ":" + timedStackTrace.stackTrace);
                }
            }
            return jsonMap;
        } catch (Exception e) {
            log.warn("Exception making JSON for slow queries", e);
            return Collections.emptyList();
        }
    }

    private static String printStacktrace(StackTraceElement[] traceElements, int offset) {
        StackTraceElement[] stackTrace = traceElements;
        StringWriter s = new StringWriter(1024);
        int i = 0;
        for (StackTraceElement traceElement : stackTrace){
            if(++i < offset){
                continue;
            }
            s.append("\tat " + traceElement).append("\n");
        }
        return s.toString();
    }

    /**
     * A struct that stores a record of a stack trace with associated latency. It's Comparable so we 
     * can keep a sorted set of the highest latencies.
     */
    private static class TimedStackTrace implements Comparable<TimedStackTrace> {
        public final String stackTrace;
        public final long latency;
        
        public TimedStackTrace(StackTraceElement[] stackTrace, long latency) {
            //Remove the top n elements, since the method traces will be from the measurement code
            this.stackTrace = printStacktrace(stackTrace, 6);
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
