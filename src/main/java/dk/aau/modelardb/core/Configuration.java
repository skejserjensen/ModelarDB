/* Copyright 2018-2020 Aalborg University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dk.aau.modelardb.core;

import java.util.*;

public class Configuration {

    /** Constructors **/
    public Configuration() {
        this.values = new HashMap<>();
        self = this;
    }

    /** Public Methods **/
    static public Configuration get() {
        if (self == null) {
            throw new IllegalStateException("CORE: a configuration object have not been constructed");
        }
        return self;
    }

    public void add(String name, Object value) {

        Object[] values;
        if (value instanceof String) {
            value = this.infer((String) value);
            this.validate(name, value);
            values = new Object[]{value};
        }  else if (value.getClass().isArray()) {
            values = (Object[]) value;
        } else {
            values = new Object[]{value};
        }
        this.values.merge(name, values, this::mergeArrays);
    }

    public boolean contains(String value) {
        return this.values.containsKey(value);
    }

    //Generic Getters
    public Object get(String name) {
        return this.values.get(name);
    }

    public String getString(String name) {
        return (String) getObject(name);
    }

    public boolean getBoolean(String name) {
        return (boolean) getObject(name);
    }

    public int getInteger(String name) {
        return (int) getObject(name);
    }

    public float getFloat(String name) {
        try {
            return (float) getObject(name);
        } catch (ClassCastException cce) {
            return (int) getObject(name);
        }
    }

    public String[] getArray(String name) {
        return Arrays.stream(this.values.get(name)).toArray(String[]::new);
    }

    //Specific Getters
    public float getError() {
        return getFloat("modelardb.error");
    }

    public int getLatency() {
        return getInteger("modelardb.latency");
    }

    public int getLimit() {
        return getInteger("modelardb.limit");
    }

    public Calendar getCalendar() {
        //Initializes the calendar with the appropriate time zone and locale
        Locale locale = new Locale(this.getString("modelardb.locale"));
        TimeZone timeZone = TimeZone.getTimeZone(this.getString("modelardb.timezone"));
        return Calendar.getInstance(timeZone, locale);
    }

    public int getResolution() {
        return getInteger("modelardb.resolution");
    }

    public String[] getModels() {
        return getArray("modelardb.model");
    }

    public String[] getDataSources() {
        return getArray("modelardb.source");
    }

    public Dimensions getDimensions() {
        return (Dimensions) getObject("modelardb.dimensions");
    }

    public String getStorage() {
        return getString("modelardb.storage");
    }

    /** Private Methods **/
    private Object infer(String value) {
        //Attempts to infer a more concrete type for the string
        if (value.equalsIgnoreCase("true")) {
            return true;
        } else if (value.equalsIgnoreCase("false")) {
            return false;
        }

        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
        }

        try {
            return Float.parseFloat(value);
        } catch (Exception e) {
        }

        return value;
    }

    private void validate(String key, Object value) {
        //Settings used by the core are checked to ensure their values are within the expected range
        switch (key) {
            case "modelardb.batch":
                if ( ! (value instanceof Integer) || (int) value <= 0) {
                    throw new IllegalArgumentException("CORE: modelardb.batch must be a positive number");
                }
                break;
            case "modelardb.error":
                if (( ! (value instanceof Float) || (float) value < 0.0 || 100.0 < (float) value) &&
                        ( ! (value instanceof Integer) || (int) value < 0.0 || 100.0 < (int) value)) {
                    throw new IllegalArgumentException("CORE: modelardb.error must be a percentage written from 0.0 to 100.0");
                }
                break;
            case "modelardb.latency":
                if ( ! (value instanceof Integer) || (int) value < 0) {
                    throw new IllegalArgumentException("CORE: modelardb.latency must be a positive number of seconds or zero to disable");
                }
                break;
            case "modelardb.limit":
                if ( ! (value instanceof Integer) || (int) value < 0) {
                    throw new IllegalArgumentException("CORE: modelardb.limit must be a positive number of seconds or zero to disable");
                }
                break;
            case "modelardb.resolution":
                if ( ! (value instanceof Integer) || (int) value <= 0) {
                    throw new IllegalArgumentException("CORE: modelardb.resolution must be a positive number of seconds");
                }
                break;
        }
    }

    private Object[] mergeArrays(Object[] a, Object[] b) {
        int j = 0;
        Object[] c = new Object[a.length + b.length];

        for (Object o : a) {
            c[j] = o;
            j++;
        }

        for (Object o : b) {
            c[j] = o;
            j++;
        }
        return c;
    }

    private Object getObject(String name) {
        Object[] values = this.values.get(name);
        if (values == null) {
            throw new UnsupportedOperationException("CORE: configuration \"" + name + "\" does not exist");
        }

        if (values.length > 1) {
            throw new UnsupportedOperationException("CORE: configuration \"" + name + "\" is not unique");
        }
        return values[0];
    }

    /** Instance Variables **/
    private final HashMap<String, Object[]> values;
    private static Configuration self = null;
}
