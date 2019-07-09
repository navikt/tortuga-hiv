package no.nav.opptjening.hiv.sekvensnummer;

import java.util.function.Function;

public class Cache<K, V> implements Function<K, V> {
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
    }

    private boolean cacheNeedsRefresh(K key) {
        return cachedKey == null || !cachedKey.equals(key);
    }
}
