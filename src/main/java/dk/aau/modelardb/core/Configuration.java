/* Copyright 2018 The ModelarDB Contributors
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

import dk.aau.modelardb.core.utility.Pair;
import dk.aau.modelardb.core.utility.ValueFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;

public class Configuration {

    /** Constructors **/
    public Configuration() {
        this.values = new HashMap<>();
    }

    /** Public Methods **/
    public Object add(String name, Object value) {

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
        return value;
    }

    public Object[] remove(String value) {
        return this.values.remove(value);
    }

    public boolean contains(String... values) {
        for (String value : values) {
            if ( ! this.values.containsKey(value)) {
                return false;
            }
        }
        return true;
    }

    public void containsOrThrow(String... values) {
        //All of the missing values should shown as one error
        ArrayList<String> missingValues = new ArrayList<>();
        for (String value : values) {
            if ( ! this.values.containsKey(value)) {
                missingValues.add(value);
            }
        }

        //If any of the values are missing execution cannot continue
        if ( ! missingValues.isEmpty()) {
            throw new IllegalArgumentException("ModelarDB: the following required options are not in the configuration file " +
                    String.join(" ", missingValues));
        }
    }

    //Generic Getters
    public Object[] get(String name) {
        return this.values.get(name);
    }

    public String getString(String name) {
        return (String) getObject(name);
    }

    public String getString(String name, String defaultValue) {
        if ( ! this.values.containsKey(name)) {
            return defaultValue;
        }
        return getString(name);
    }

    public boolean getBoolean(String name) {
        return (boolean) getObject(name);
    }

    public boolean getBoolean(String name, boolean defaultValue) {
        if ( ! this.values.containsKey(name)) {
            return defaultValue;
        }
        return getBoolean(name);
    }

    public int getInteger(String name) {
        return (int) getObject(name);
    }

    public int getInteger(String name, int defaultValue) {
        if ( ! this.values.containsKey(name)) {
            return defaultValue;
        }
        return getInteger(name);
    }

    public float getFloat(String name) {
        try {
            return (float) getObject(name);
        } catch (ClassCastException cce) {
            return (int) getObject(name);
        }
    }

    public float getFloat(String name, int defaultValue) {
        if ( ! this.values.containsKey(name)) {
            return defaultValue;
        }
        return getFloat(name);
    }

    public String[] getArray(String name) {
        return Arrays.stream(this.values.get(name)).toArray(String[]::new);
    }

    //Specific Getters
    public int getBatchSize() {
        return getInteger("modelardb.batch_size");
    }

    public HashMap<Integer, Pair<String, ValueFunction>[]> getDerivedTimeSeries() {
        return (HashMap<Integer, Pair<String, ValueFunction>[]>) this.values.get("modelardb.sources.derived")[0];
    }

    public Correlation[] getCorrelations() {
        return (Correlation[]) this.values.get("modelardb.correlations");
    }

    public Dimensions getDimensions() {
        return (Dimensions) getObject("modelardb.dimensions");
    }

    public float getErrorBound() {
        return getFloat("modelardb.error_bound", 0);
    }

    public ExecutorService getExecutorService() {
        return (ExecutorService) getObject("modelardb.executor_service");
    }

    public int getIngestors() {
        return getInteger("modelardb.ingestors", 0);
    }

    public int getLengthBound() {
        return getInteger("modelardb.length_bound", 50);
    }

    public String[] getModelTypeNames() {
        return getArray("modelardb.model_types");
    }

    public int getMaximumLatency() {
        return getInteger("modelardb.maximum_latency", 0);
    }

    public int getSamplingInterval() {
        return getInteger("modelardb.sampling_interval");
    }

    public String[] getSources() {
        return getArray("modelardb.sources");
    }

    public String getStorage() {
        return getString("modelardb.storage");
    }

    public TimeZone getTimeZone() {
        if (values.containsKey("modelardb.time_zone")) {
            return TimeZone.getTimeZone((String) values.get("modelardb.time_zone")[0]);
        } else {
            return TimeZone.getDefault();
        }
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
        } catch (Exception ignored) {
        }

        try {
            return Float.parseFloat(value);
        } catch (Exception ignored) {
        }

        return value;
    }

    private void validate(String key, Object value) {
        //Settings used by the core are checked to ensure their values are within the expected range
        switch (key) {
            case "modelardb.batch_size":
                if ( ! (value instanceof Integer) || (int) value <= 0) {
                    throw new IllegalArgumentException("CORE: modelardb.batch_size must be a positive number");
                }
                break;
            case "modelardb.error_bound":
                if ( ! (value instanceof Float) && ! (value instanceof Integer)) {
                    throw new IllegalArgumentException("CORE: modelardb.error_bound must be an integer or a float");
                }
                break;
            case "modelardb.maximum_latency":
                if ( ! (value instanceof Integer) || (int) value < 0) {
                    throw new IllegalArgumentException("CORE: modelardb.maximum_latency must be zero or more data point groups");
                }
                break;
            case "modelardb.length_bound":
                if ( ! (value instanceof Integer) || (int) value <= 0) {
                    throw new IllegalArgumentException("CORE: modelardb.length_bound must be a positive number of data point groups");
                }
                break;
            case "modelardb.sampling_interval":
                if ( ! (value instanceof Integer) || (int) value < 0) {
                    throw new IllegalArgumentException("CORE: modelardb.sampling_interval must be zero or a positive number of seconds");
                }
                break;
            case "modelardb.ingestors":
                if ( ! (value instanceof Integer) || (int) value < 0) {
                    throw new IllegalArgumentException("ModelarDB: modelardb.ingestors must be zero or a positive number of ingestors");
                }
                break;
            case "modelardb.time_zone":
                if ( ! (value instanceof String) || ! TimeZone.getTimeZone((String) value).getID().equals(value)) {
                    throw new IllegalArgumentException("ModelarDB: modelardb.time_zone must be a valid time zone id");
                }
                break;
            case "modelardb.dynamic_split_fraction":
                if ( ! (value instanceof Float) || (float) value < 0.0) {
                    throw new IllegalArgumentException("ModelarDB: modelardb.dynamic_split_fraction must be zero or a positive float");
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
}