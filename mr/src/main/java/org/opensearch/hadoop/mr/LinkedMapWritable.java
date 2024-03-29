/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
 
/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.opensearch.hadoop.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Similar to {@link MapWritable} expect that it uses a {@link LinkedHashMap} underneath to preserve insertion order (and thus its structure). Extends {@link MapWritable} for compatibility reasons.
 */
public class LinkedMapWritable extends MapWritable {

    private Map<Writable, Writable> instance;

    /** Default constructor. */
    public LinkedMapWritable() {
        this.instance = new LinkedHashMap<Writable, Writable>();
        addToMap(LinkedMapWritable.class);
    }

    /**
     * Copy constructor.
     *
     * @param other the map to copy from
     */
    public LinkedMapWritable(MapWritable other) {
        this();
        copy(other);
    }

    /** {@inheritDoc} */
    public void clear() {
        instance.clear();
    }

    /** {@inheritDoc} */
    public boolean containsKey(Object key) {
        return instance.containsKey(key);
    }

    /** {@inheritDoc} */
    public boolean containsValue(Object value) {
        return instance.containsValue(value);
    }

    /** {@inheritDoc} */
    public Set<Map.Entry<Writable, Writable>> entrySet() {
        return instance.entrySet();
    }

    /** {@inheritDoc} */
    public Writable get(Object key) {
        return instance.get(key);
    }

    /** {@inheritDoc} */
    public boolean isEmpty() {
        return instance.isEmpty();
    }

    /** {@inheritDoc} */
    public Set<Writable> keySet() {
        return instance.keySet();
    }

    /** {@inheritDoc} */
    public Writable put(Writable key, Writable value) {
        addToMap(key.getClass());
        addToMap(value.getClass());
        return instance.put(key, value);
    }

    /** {@inheritDoc} */
    public void putAll(Map<? extends Writable, ? extends Writable> t) {
        for (Map.Entry<? extends Writable, ? extends Writable> e : t.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    /** {@inheritDoc} */
    public Writable remove(Object key) {
        return instance.remove(key);
    }

    /** {@inheritDoc} */
    public int size() {
        return instance.size();
    }

    /** {@inheritDoc} */
    public Collection<Writable> values() {
        return instance.values();
    }

    // Writable

    /** {@inheritDoc} */
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        // Write out the number of entries in the map

        out.writeInt(instance.size());

        // Then write out each key/value pair

        for (Map.Entry<Writable, Writable> e : instance.entrySet()) {
            out.writeByte(getId(e.getKey().getClass()));
            e.getKey().write(out);
            out.writeByte(getId(e.getValue().getClass()));
            e.getValue().write(out);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        // First clear the map.  Otherwise we will just accumulate
        // entries every time this method is called.
        this.instance.clear();

        // Read the number of entries in the map

        int entries = in.readInt();

        // Then read each key/value pair

        for (int i = 0; i < entries; i++) {
            Writable key = (Writable) ReflectionUtils.newInstance(getClass(in.readByte()), getConf());

            key.readFields(in);

            Writable value = (Writable) ReflectionUtils.newInstance(getClass(in.readByte()), getConf());

            value.readFields(in);
            instance.put(key, value);
        }
    }

    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
        if (o == this)
            return true;

        if (!(o instanceof Map))
            return false;
        Map<Writable, Writable> m = (Map<Writable, Writable>) o;
        if (m.size() != size())
            return false;

        try {
            Iterator<Entry<Writable, Writable>> i = entrySet().iterator();
            while (i.hasNext()) {
                Entry<Writable, Writable> e = i.next();
                Writable key = e.getKey();
                Writable value = e.getValue();
                if (value == null) {
                    if (!(m.get(key) == null && m.containsKey(key)))
                        return false;
                }
                else {
                    if (!value.equals(m.get(key)))
                        return false;
                }
            }
        } catch (ClassCastException unused) {
            return false;
        } catch (NullPointerException unused) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int h = 0;
        Iterator<Entry<Writable, Writable>> i = entrySet().iterator();
        while (i.hasNext())
            h += i.next().hashCode();
        return h;
    }

    @Override
    public String toString() {
        Iterator<Entry<Writable, Writable>> i = entrySet().iterator();
        if (!i.hasNext())
            return "{}";

        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (;;) {
            Entry<Writable, Writable> e = i.next();
            Writable key = e.getKey();
            Writable value = e.getValue();
            sb.append(key == this ? "(this Map)" : key);
            sb.append('=');
            if (value instanceof ArrayWritable) {
                sb.append(Arrays.toString(((ArrayWritable) value).get()));
            }
            else {
                sb.append(value == this ? "(this Map)" : value);
            }
            if (!i.hasNext())
                return sb.append('}').toString();
            sb.append(", ");
        }
    }
}