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

import dk.aau.modelardb.core.models.Model;
import dk.aau.modelardb.core.models.ModelFactory;
import dk.aau.modelardb.core.utility.Static;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;

public abstract class Storage {

    /** Public Methods **/
    abstract public void open(Dimensions dimensions);
    abstract public int getMaxSID();
    abstract public int getMaxGID();
    abstract public void initialize(TimeSeriesGroup[] timeSeriesGroups, Dimensions dimensions, String[] modelNames);
    abstract public void close();
    abstract public void insert(SegmentGroup[] segments, int length);
    abstract public Stream<SegmentGroup> getSegments();

    public HashMap<String, Integer> getMidCache() {
        return this.midCache;
    }

    public Model[] getModelCache() {
        return this.modelCache;
    }

    public int[] getSourceGroupCache() {
        return this.sourceGroupCache;
    }

    public float[] getSourceScalingFactorCache() {
        return this.sourceScalingFactorCache;
    }

    public Dimensions getDimensions() {
        return this.dimensions;
    }

    public Object[][] getDimensionsCache() {
        return this.dimensionsCache;
    }

    public HashMap<String, HashMap<Object, Integer[]>> getInverseDimensionsCache() {
        return this.inverseDimensionsCache;
    }

    public int[][] getGroupMetadataCache() {
        return this.groupMetadataCache;
    }

    /** Protected Methods **/
    protected HashMap<String, Integer> initializeCaches(String[] modelNames,
                                                        Dimensions dimensions,
                                                        HashMap<String, Integer> modelsInStorage,
                                                        HashMap<Integer, Object[]> sourcesInStorage) {

        //The Dimensions object is stored so the schema can be retrieved later
        this.dimensions = dimensions;

        //Computes the set of models that must be inserted for the system to
        // function, per definition the mid of the fallback model is one
        HashMap<String, Integer> modelsToBeInserted = new HashMap<>();
        List<String> modelsWithFallback = new ArrayList<>(Arrays.asList(modelNames));
        modelsWithFallback.add(0, "dk.aau.modelardb.core.models.UncompressedModel");
        int mid = modelsInStorage.values().stream().max(Integer::compare).orElse(0);
        for (String model : modelsWithFallback) {
            if ( ! modelsInStorage.containsKey(model)) {
                mid += 1;
                modelsInStorage.put(model, mid);
                modelsToBeInserted.put(model, mid);
            }
        }

        //Verifies that the implementation of all models are loaded and creates the midCache and modelCache
        this.modelCache = new Model[modelsInStorage.size() + 1];
        this.midCache = new HashMap<>();
        for (Entry<String, Integer> entry : modelsInStorage.entrySet()) {
            Model model = ModelFactory.getModel(entry.getKey(), 0,0.0F, 0);
            mid = entry.getValue();
            this.modelCache[mid] = model;
            this.midCache.put(model.getClass().getName(), mid);
        }

        //Creates the sourceGroupCache, the dimensionsCache, and the inverseDimensionsCache
        int sourcesCachesSize = getMaxSID() + 1;
        this.sourceGroupCache = new int[sourcesCachesSize];
        this.sourceScalingFactorCache = new float[sourcesCachesSize];
        this.dimensionsCache = new Object[sourcesCachesSize][];
        HashMap<Integer, ArrayList<Integer>> gsc = new HashMap<>();
        sourcesInStorage.forEach((sid, metadata) -> {
            //Metadata is a mapping from Sid to Scaling, Resolution, Gid, and Dimensions
            int gid = (int) metadata[2];
            this.sourceGroupCache[sid] = gid;
            this.sourceScalingFactorCache[sid] = (float) metadata[0];
            if ( ! gsc.containsKey(gid)) {
                //A group consist of time series with equivalent SI so we just store it once
                ArrayList<Integer> metadataArray = new ArrayList<>();
                metadataArray.add((int) metadata[1]);
                gsc.put(gid, metadataArray);
            }
            gsc.get(gid).add(sid);

            int dim = 0;
            Object[] columns = new Object[metadata.length - 3];
            for (int i = 3; i < metadata.length; i++) {
                columns[dim] = metadata[i];
                dim++;
            }
            this.dimensionsCache[sid] = columns;
        });

        //The inverseDimensionsCache is constructed from the dimensions cache
        String[] columns = this.dimensions.getColumns();
        HashMap<String, HashMap<Object, HashSet<Integer>>> outer = new HashMap<>();
        for (int i = 1; i < this.dimensionsCache.length; i++) {
            //If data with existing sids are ingested missing sids can occur and must be handled
            if (this.dimensionsCache[i] == null) {
                Static.warn(String.format("CORE: a time series with sid %d does not exist", i));
                continue;
            }

            for (int j = 0; j < columns.length; j++) {
                Object value = this.dimensionsCache[i][j];
                HashMap<Object, HashSet<Integer>> inner = outer.getOrDefault(columns[j], new HashMap<>());
                HashSet<Integer> sids = inner.getOrDefault(value, new HashSet<>());
                sids.add(this.sourceGroupCache[i]);
                inner.put(value, sids);
                outer.put(columns[j], inner);
            }
        }

        this.inverseDimensionsCache = new HashMap<>();
        for (Entry<String, HashMap<Object, HashSet<Integer>>> oes : outer.entrySet()) {
            HashMap<Object, Integer[]> innerAsArray = new HashMap<>();
            for (Entry<Object, HashSet<Integer>> ies : oes.getValue().entrySet()) {
                Integer[] inner = ies.getValue().toArray(new Integer[0]);
                Arrays.sort(inner); //Sorted to make it simpler to read when debugging
                innerAsArray.put(ies.getKey(), inner);
            }
            this.inverseDimensionsCache.put(oes.getKey(), innerAsArray);
        }

        //Finally the sorted groupMetadataCache is created and consists of resolution and sids
        this.groupMetadataCache = new int[gsc.size() + 1][];
        gsc.forEach((k,v) -> {
            this.groupMetadataCache[k] = v.stream().mapToInt(i -> i).toArray();
            Arrays.sort(this.groupMetadataCache[k], 1, this.groupMetadataCache[k].length);
        });
        return modelsToBeInserted;
    }

    /** Instance Variables **/
    protected Dimensions dimensions;

    //Model Construction Cache: Maps the name of a model to the corresponding mid used in the data store
    protected HashMap<String, Integer> midCache;

    //Read Cache: Maps the mid of a model to an instance of the model so a segment can be constructed
    protected Model[] modelCache;

    //Read Cache: Maps the sid of a source to the gid of the group that the source is a member of
    protected int[] sourceGroupCache;

    //Read Cache: Maps the sid of a source to the scaling factor specified for that source
    protected float[] sourceScalingFactorCache;

    //Read Cache: Maps the sid of a source to the members provided for that data source
    protected Object[][] dimensionsCache;

    //Read Cache: Maps the value of a column for a dimension to the gids with that member
    protected HashMap<String, HashMap<Object, Integer[]>> inverseDimensionsCache;

    //Read Cache: Maps the gid of a group to the groups resolution and the sids that are part of that group
    protected int[][] groupMetadataCache;
}
