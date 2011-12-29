package com.urbanairship.statshtable;

import java.io.ByteArrayOutputStream;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.yammer.metrics.core.GaugeMetric;

/**
 * 
 */
public class SlowestQueryGauge implements GaugeMetric<String> {
    private static final Logger log = LogManager.getLogger(SlowestQueryGauge.class);
    
    NavigableMap<Long,String> slowQueries = new TreeMap<Long,String>();
    private ObjectMapper mapper;
    private int howMany;
    
    public SlowestQueryGauge(int howMany) {
        this.howMany = howMany;
    }
    
    synchronized public void update(long latencyMs, String label) {
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
            slowQueries.put(latencyMs, label);
            if(slowQueries.size() > howMany) {
                slowQueries.remove(slowQueries.firstKey());
            }
        }
    }

    @Override
    public String value() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(slowQueries.size() * 100 + 5);
        try {
            synchronized (this) {
                mapper.writeValue(bos, slowQueries);
            }
            return bos.toString();
        } catch (Exception e) {
            log.warn("Exception making JSON for slow queries", e);
            return "exception";
        }
    }
}
