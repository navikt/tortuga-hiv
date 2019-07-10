package no.nav.opptjening.hiv.sekvensnummer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class Cache<K, V> implements Function<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(Cache.class);

    private V cachedValue;
    private K cachedKey;
    private Function<K, V> cachedMethod;

    Cache(Function<K, V> cachedMethod) {
        this.cachedMethod = cachedMethod;
    }

    @Override
    public V apply(K key) {
        if (cacheNeedsRefresh(key)) {
            updateCache(key);
        }
        return cachedValue;
    }

    private void updateCache(K key) {
        cachedValue = cachedMethod.apply(key);
        cachedKey = key;
        LOG.info("Cache updated. Key: {}, Value: {}", cachedKey, cachedValue);
    }

    private boolean cacheNeedsRefresh(K key) {
        return cachedKey == null || !cachedKey.equals(key);
    }
}
