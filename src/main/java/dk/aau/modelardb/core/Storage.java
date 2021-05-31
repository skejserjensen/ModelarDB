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

import dk.aau.modelardb.core.models.ModelType;
import dk.aau.modelardb.core.models.ModelTypeFactory;
import dk.aau.modelardb.core.utility.Pair;
import dk.aau.modelardb.core.utility.Static;
import dk.aau.modelardb.core.utility.ValueFunction;

import java.util.*;
import java.util.Map.Entry;

public abstract class Storage {

    /** Public Methods **/
    abstract public void open(Dimensions dimensions);
    abstract public void initialize(TimeSeriesGroup[] timeSeriesGroups,
                                    HashMap<Integer, Pair<String, ValueFunction>[]> derivedTimeSeries,
                                    Dimensions dimensions, String[] modelNames);
    abstract public int getMaxTid();
    abstract public int getMaxGid();
    abstract public void close();

    /** Protected Methods **/
    protected HashMap<String, Integer> initializeCaches(String[] modelNames,
                                                        Dimensions dimensions,
                                                        HashMap<String, Integer> modelTypesInStorage,
                                                        HashMap<Integer, Object[]> timeSeriesInStorage,
                                                        HashMap<Integer, Pair<String, ValueFunction>[]> derivedTimeSeries) {

        //The Dimensions object is stored so the schema can be retrieved later
        this.dimensions = dimensions;

        //Computes the set of model types that must be inserted for the system to
        // function, per definition the mtid of the fallback model type is one
        HashMap<String, Integer> modelTypesToBeInserted = new HashMap<>();
        List<String> modelsWithFallback = new ArrayList<>(Arrays.asList(modelNames));
        modelsWithFallback.add(0, "dk.aau.modelardb.core.models.UncompressedModelType");
        int mtid = modelTypesInStorage.values().stream().max(Integer::compare).orElse(0);
        for (String model : modelsWithFallback) {
            if ( ! modelTypesInStorage.containsKey(model)) {
                mtid += 1;
                modelTypesInStorage.put(model, mtid);
                modelTypesToBeInserted.put(model, mtid);
            }
        }

        //Verifies that the implementation of all model types are loaded and creates the modelTypeCache and mtidCache
        this.modelTypeCache = new ModelType[modelTypesInStorage.size() + 1];
        this.mtidCache = new HashMap<>();
        for (Entry<String, Integer> entry : modelTypesInStorage.entrySet()) {
            ModelType modelType = ModelTypeFactory.getModel(entry.getKey(), 0,0.0F, 0);
            mtid = entry.getValue();
            this.modelTypeCache[mtid] = modelType;
            this.mtidCache.put(modelType.getClass().getName(), mtid);
        }

        //Creates the timeSeriesGroupCache, timeSeriesScalingFactorCache, and timeSeriesMembersCache
        int nextTid = getMaxTid() + 1;
        int totalNumberOfSources = nextTid + derivedTimeSeries.values().stream().mapToInt(v -> v.length).sum();
        this.timeSeriesGroupCache = new int[totalNumberOfSources];
        this.timeSeriesSamplingIntervalCache = new int[totalNumberOfSources];
        this.timeSeriesScalingFactorCache = new float[totalNumberOfSources];
        this.timeSeriesMembersCache = new Object[totalNumberOfSources][];
        HashMap<Integer, ArrayList<Integer>> gsc = new HashMap<>();
        ValueFunction scalingTransformation = new ValueFunction();
        this.timeSeriesTransformationCache = new ValueFunction[totalNumberOfSources];
        HashMap<Integer, ArrayList<Integer>> groupDerivedCacheBuilder = new HashMap<>();
        for (Entry<Integer, Object[]> entry : timeSeriesInStorage.entrySet()) {
            //Metadata is a mapping from Tid to Scaling, Sampling Interval, Gid, and Dimensions
            Integer tid = entry.getKey();
            Object[] metadata = entry.getValue();

            //Creates mappings from tid -> gid, tid -> sampling interval, tid -> scaling factor, and tid -> dimensions
            int gid = (int) metadata[2];
            this.timeSeriesGroupCache[tid] = gid;
            this.timeSeriesSamplingIntervalCache[tid] = (int) metadata[1];
            this.timeSeriesScalingFactorCache[tid] = (float) metadata[0];
            if ( ! gsc.containsKey(gid)) {
                //A group consist of time series with equivalent SI
                ArrayList<Integer> metadataArray = new ArrayList<>();
                metadataArray.add((int) metadata[1]);
                gsc.put(gid, metadataArray);
            }
            gsc.get(gid).add(tid);

            int dim = 0;
            Object[] columns = new Object[metadata.length - 3];
            for (int i = 3; i < metadata.length; i++) {
                columns[dim] = metadata[i];
                dim++;
            }
            this.timeSeriesMembersCache[tid] = columns;
            this.timeSeriesTransformationCache[tid] = scalingTransformation;

            //Creates mappings from gid -> pair of tids for original and derived (gdc), and from tid -> to transformation (tc)
            if (derivedTimeSeries.containsKey(tid)) {
                //All derived time series perform all of their transformations in their user-defined function
                Pair<String, ValueFunction>[] sourcesAndTransformations = derivedTimeSeries.get(tid);
                ArrayList<Integer> gdcb = groupDerivedCacheBuilder.computeIfAbsent(gid, g -> new ArrayList<>());
                for (Pair<String, ValueFunction> sat : sourcesAndTransformations) {
                    int dtid = nextTid++;
                    this.timeSeriesGroupCache[dtid] = gid;
                    this.timeSeriesSamplingIntervalCache[dtid] = (int) metadata[1];
                    this.timeSeriesScalingFactorCache[dtid] = 1.0F; //HACK: scaling is assumed to be part of the transformation
                    this.timeSeriesMembersCache[dtid] = dimensions.get(sat._1);
                    this.timeSeriesTransformationCache[dtid] = sat._2;
                    gdcb.add(tid);
                    gdcb.add(dtid);
                }
            }
        }
        this.groupDerivedCache = new HashMap<>();
        groupDerivedCacheBuilder.forEach((k, v) -> this.groupDerivedCache.put(k, v.stream().mapToInt(i -> i).toArray()));

        //The inverseDimensionsCache is constructed from the dimensions cache
        String[] columns = this.dimensions.getColumns();
        HashMap<String, HashMap<Object, HashSet<Integer>>> outer = new HashMap<>();
        for (int i = 1; i < this.timeSeriesMembersCache.length; i++) {
            //If data with existing tids are ingested missing tids can occur and must be handled
            if (this.timeSeriesMembersCache[i] == null) {
                Static.warn(String.format("CORE: a time series with tid %d does not exist", i));
                continue;
            }

            for (int j = 0; j < columns.length; j++) {
                Object value = this.timeSeriesMembersCache[i][j];
                HashMap<Object, HashSet<Integer>> inner = outer.getOrDefault(columns[j], new HashMap<>());
                HashSet<Integer> tids = inner.getOrDefault(value, new HashSet<>());
                tids.add(this.timeSeriesGroupCache[i]);
                inner.put(value, tids);
                outer.put(columns[j], inner);
            }
        }

        this.memberTimeSeriesCache = new HashMap<>();
        for (Entry<String, HashMap<Object, HashSet<Integer>>> oes : outer.entrySet()) {
            HashMap<Object, Integer[]> innerAsArray = new HashMap<>();
            for (Entry<Object, HashSet<Integer>> ies : oes.getValue().entrySet()) {
                Integer[] inner = ies.getValue().toArray(new Integer[0]);
                Arrays.sort(inner); //Sorted to make it simpler to read when debugging
                innerAsArray.put(ies.getKey(), inner);
            }
            //Some engines converts all columns to uppercase so the caches key must also be so
            this.memberTimeSeriesCache.put(oes.getKey().toUpperCase(), innerAsArray);
        }

        //Finally the sorted groupMetadataCache is created and consists of sampling interval and tids
        this.groupMetadataCache = new int[gsc.size() + 1][];
        gsc.forEach((k,v) -> {
            this.groupMetadataCache[k] = v.stream().mapToInt(i -> i).toArray();
            Arrays.sort(this.groupMetadataCache[k], 1, this.groupMetadataCache[k].length);
        });
        return modelTypesToBeInserted;
    }

    protected String getDimensionsSQL(Dimensions dimensions, String textType) {
        String[] columns = dimensions.getColumns();
        Dimensions.Types[] types = dimensions.getTypes();
        if (types.length == 0) {
            return "";
        }

        //The schema is build with a starting comma so it is easy to embed into a CREATE table statement
        StringBuilder sb = new StringBuilder();
        sb.append(", ");
        int withPunctuation = columns.length - 1;
        for (int i = 0
             ; i <  withPunctuation; i++) {
            sb.append(columns[i]);
            sb.append(' ');
            if (types[i] == Dimensions.Types.TEXT) {
                sb.append(textType);
            } else {
                sb.append(types[i].toString());
            }
            sb.append(", ");
        }
        sb.append(columns[withPunctuation]);
        sb.append(' ');
        if (types[withPunctuation] == Dimensions.Types.TEXT) {
            sb.append(textType);
        } else {
            sb.append(types[withPunctuation].toString());
        }
        return sb.toString();
    }

    /** Instance Variables **/
    public Dimensions dimensions;


    //Write Cache: Maps the name of a model type to the corresponding mtid used by the storage layer
    public HashMap<String, Integer> mtidCache;

    //Read Cache: Maps the mtid of a model type to an instance of the model type so segments can be constructed from it
    public ModelType[] modelTypeCache;


    //Read Cache: Maps the tid of a time series to the gid of the group that the time series is a member of
    public int[] timeSeriesGroupCache;

    //Read Cache: Maps the tid of a time series to the sampling interval specified for that time series
    public int[] timeSeriesSamplingIntervalCache;

    //Read Cache: Maps the tid of a time series to the scaling factor specified for for that time series
    public float[] timeSeriesScalingFactorCache;

    //Read Cache: Maps the tid of a time series to the transformation specified for that time series
    public ValueFunction[] timeSeriesTransformationCache;

    //Read Cache: Maps the tid of a time series to the members specified for that time series
    public Object[][] timeSeriesMembersCache;


    //Read Cache: Maps the value of a column for a dimension to the tids with that member
    public HashMap<String, HashMap<Object, Integer[]>> memberTimeSeriesCache;


    //Read Cache: Maps the gid of a group to the groups sampling interval and the tids that are part of that group
    public int[][] groupMetadataCache;

    //Read Cache: Maps the gid of a group to pairs of tids for time series with derived time series
    public HashMap<Integer, int[]> groupDerivedCache;
}
