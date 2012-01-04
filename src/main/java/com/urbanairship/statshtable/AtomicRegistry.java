package com.urbanairship.statshtable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Stores a thread-safe registry of metrics. There are multiple "scopes," and each scope has its own mapping
 * from metricnames to metric objects.
 *
 * This is basically a nested Map with a sort of concurrency control.
 *
 * @param <S> the type of object used for scopes. e.g. String
 * @param <N> the type of object used for metric names, e.g. a String or enum 
 * @param <M> the type of object used for metrics, e.g. TimerMetric
 */
public class AtomicRegistry<S,N,M> {
    private ConcurrentMap<S,ConcurrentMap<N,M>> map = new ConcurrentHashMap<S,ConcurrentMap<N,M>>();

    public M registerOrGet(S scope, N name, Callable<M> metricFactory, UnaryFunction<M> jmxRegisterFunc) throws Exception {
        ConcurrentMap<N,M> innerMap = map.get(scope);
        if(innerMap == null) {
            ConcurrentMap<N,M> newMap = new ConcurrentHashMap<N,M>();
            innerMap = map.putIfAbsent(scope, newMap);
            if(innerMap == null) {
                innerMap = newMap;
            }
        }
        
        M metric = innerMap.get(name);
        if(metric == null) {
            M newMetric = metricFactory.call();
            metric = innerMap.putIfAbsent(name, newMetric);
            if(metric == null) {
                metric = newMetric;
                jmxRegisterFunc.apply(metric);
            }
        }
        return metric;
    }
    
    /**
     * Looks up all the metrics for the given scope.
     * @return null if there are no metrics for the given scope.
     */
    public Map<N,M> innerMap(S scope) {
        return map.get(scope);
    }
    
    /**
     * Return a set view of the scopes contained in the registry. This is a set returned by 
     * ConcurrentHashMap.keySet() so, to quote the javadoc:
     * 
     *   The view's iterator is a "weakly consistent" iterator that will never throw 
     *   ConcurrentModificationException, and guarantees to traverse elements as they existed upon construction 
     *   of the iterator, and may (but is not guaranteed to) reflect any modifications subsequent to 
     *   construction.
     */
    public Set<S> getScopes() {
        return map.keySet();
    }
    
    public void remove(S scope, N name) {
        Map<N,M> innerMap = innerMap(scope);
        if(innerMap == null) {
            return;
        }
        innerMap.remove(name);
    }
}
