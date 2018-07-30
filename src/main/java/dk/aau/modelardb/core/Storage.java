/* Copyright 2018 Aalborg University
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

import dk.aau.modelardb.core.models.Segment;
import dk.aau.modelardb.core.models.Model;
import dk.aau.modelardb.core.models.ModelFactory;
import dk.aau.modelardb.core.utility.Pair;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;

public abstract class Storage {

    /** * Public Methods **/
    abstract public void open();
    abstract public int getMaxSID();
    abstract public void init(TimeSeries[] timeSeries, String[] models);
    abstract public void close();
    abstract public void insert(Segment[] segments, int length);
    abstract public Stream<Segment> getSegments();

    public Segment constructSegment(int sid, long startTime, long endTime, int mid, byte[] parameters, byte[] gaps) {
        int resolution = this.resolutionCache[sid];
        Model m = this.modelCache[mid];
        return m.get(sid, startTime, endTime, resolution, parameters, gaps);
    }

    public HashMap<String, Integer> getMidCache() {
        return this.midCache;
    }

    public Model[] getModelCache() {
        return this.modelCache;
    }

    public int[] getResolutionCache() {
        return this.resolutionCache;
    }

    /** Protected Methods **/
    protected Pair<HashMap<String, Integer>, HashMap<Integer, Integer>> initCaches(TimeSeries[] timeSeries,
                                                                                   String[] modelNames,
                                                                                   HashMap<String, Integer> modelsInStorage,
                                                                                   HashMap<Integer, Integer> sourcesInStorage) {

        //Verify that the uncompressed model is in storage as it is used as a fallback if nothing else is ready
        String fallbackModel = "dk.aau.modelardb.core.models.UncompressedModel";
        List<String> mn = new ArrayList<>(Arrays.asList(modelNames));
        if ( ! mn.contains(fallbackModel)) {
            mn.add(fallbackModel);
            modelNames = mn.toArray(new String[0]);
        }

        //The set of segments and time series in the config but not in storage and their associated unused identifiers
        HashMap<String, Integer> newModelsInConf = new HashMap<>();
        HashMap<Integer, Integer> newTimeSeriesInConf = new HashMap<>();

        //Verify that all models and associated segments in the data store is loaded in the JVM
        for (Entry<String, Integer> model : modelsInStorage.entrySet()) {
            ModelFactory.getModel(model.getKey(), 0.0F, 0);
        }

        //Determine the current maximum id used for the models in storage
        Integer currentMaxMid = 0;
        if ( ! modelsInStorage.isEmpty()) {
            currentMaxMid = Collections.max(modelsInStorage.values());
        }

        //Union the segments from the database with the segment types from the config
        Model[] models = ModelFactory.getModels(modelNames, 0.0F, 0);
        for (Model model : models) {
            String modelName = model.getClass().getName();

            if ( ! modelsInStorage.containsKey(modelName)) {
                currentMaxMid += 1;
                modelsInStorage.put(modelName, currentMaxMid);
                newModelsInConf.put(modelName, currentMaxMid);
            }
        }

        /* Setup midCache and modelCache */
        this.modelCache = new Model[modelsInStorage.size() + 1];
        this.midCache = new HashMap<>();
        for (Entry<String, Integer> entry : modelsInStorage.entrySet()) {
            int mid = entry.getValue();
            Model model = ModelFactory.getModel(entry.getKey(), 0.0F, 0);
            this.modelCache[mid] = model;
            this.midCache.put(model.segment.getName(), mid);
        }

        /* Setup resolutionCache */
        for (TimeSeries ts : timeSeries) {
            sourcesInStorage.put(ts.sid, ts.resolution);
            newTimeSeriesInConf.put(ts.sid, ts.resolution);
        }
        this.resolutionCache = new int[sourcesInStorage.size() + 1];
        sourcesInStorage.forEach((k,v) -> this.resolutionCache[k] = v);

        return new Pair<>(newModelsInConf, newTimeSeriesInConf);
    }

    /** Instance Variables **/
    //Insert Cache: Maps the name of a segment to the corresponding mid used in the data store
    protected HashMap<String, Integer> midCache;

    //Read Cache: Maps the mid of a model to an instance of the model so a segment can be constructed
    protected Model[] modelCache;

    //Read Cache: Maps the sid of a source to the metadata necessary for reconstruction which is resolution
    protected int[] resolutionCache;
}