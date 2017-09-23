package de.uhh.lt.lefex.Utils;

import java.util.HashMap;

public class DefaultDict<K, V> extends HashMap<K, V> {

    Class<V> klass;
    public DefaultDict(Class klass) {
        this.klass = klass;
    }

    @Override
    public V get(Object key) {
        V returnValue = super.get(key);
        if (returnValue == null) {
            try {
                returnValue = klass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            this.put((K) key, returnValue);
        }
        return returnValue;
    }
}