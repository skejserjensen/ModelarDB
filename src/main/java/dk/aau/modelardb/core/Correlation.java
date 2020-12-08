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

import dk.aau.modelardb.core.utility.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class Correlation {

    /** Constructors **/
    public Correlation() {
        this.distance = -1.0F;

        this.correlatedSources = new HashSet<>();
        this.correlatedMembers = new HashMap<>();
        this.correlatedDimensions = new HashMap<>();

        this.scalingFactorForMember = new HashMap<>();
        this.scalingFactorForSource = new HashMap<>();
    }

    /** Public Methods **/
    public boolean test(TimeSeries[] groupOne, TimeSeries[] groupTwo, Dimensions dimensions) {
        return isCorrelatedBySources(groupOne, groupTwo) &&
                isCorrelatedByMembers(groupOne, groupTwo, dimensions) &&
                isCorrelatedByLCALevels(groupOne, groupTwo, dimensions) &&
                isCorrelatedByDistance(groupOne, groupTwo, dimensions);
    }

    public void updateScalingFactors(TimeSeries[] group, Dimensions dimensions) {
        for (TimeSeries ts : group) {
            //Sets the scaling factor for specific time series
            for (HashMap.Entry<String, Float> ls : this.scalingFactorForSource.entrySet()) {
                if (ls.getKey().equals(ts.source)) {
                    ts.setScalingFactor(ls.getValue());
                }
            }

            //Sets the scaling factor for time series with specific members
            for (HashMap.Entry<Integer, HashMap<Object, Float>> level : this.scalingFactorForMember.entrySet()) {
                for (HashMap.Entry<Object, Float> member : level.getValue().entrySet()) {
                    Object tsMember = dimensions.get(ts.source)[level.getKey()];
                    if (tsMember.equals(member.getKey())) {
                        ts.setScalingFactor(member.getValue());
                    }
                }
            }
        }
    }

    public void setDistance(float distance) {
        this.distance = distance;
    }

    public void addSources(String[] sources) {
        //Elements inside a clause are combined with an AND operator, so multiple sets of sources are an error
        if ( ! this.correlatedSources.isEmpty()) {
            throw new IllegalArgumentException("CORE: correlated sources have already been declared in this clause");
        }
        this.correlatedSources.addAll(Arrays.asList(sources));
    }

    public boolean hasOnlyCorrelatedSources() {
        return this.distance == -1.0F && ! this.correlatedSources.isEmpty() &&
                this.correlatedMembers.isEmpty() && this.correlatedDimensions.isEmpty() &&
                this.scalingFactorForMember.isEmpty() && this.scalingFactorForSource.isEmpty();
    }

    public HashSet<String> getCorrelatedSources() {
        return this.correlatedSources;
    }

    public void addDimensionAndMembers(String dim, Integer level, String[] members, Dimensions dimensions) {
        int column = getColumn(dim, level, dimensions);
        if ( ! this.correlatedMembers.containsKey(column)) {
            this.correlatedMembers.put(column, new HashSet<>());
        } else {
            //Elements inside a clause are combined with an AND operator, so multiple sets of members are an error
            throw new IllegalArgumentException("CORE: correlation have already been declared for this column in this clause");
        }
        Object[] parsedMember = Arrays.stream(members).map(member -> dimensions.parse(column, member)).toArray();
        this.correlatedMembers.get(column).addAll(Arrays.asList(parsedMember));
    }

    public void addDimensionAndLCA(String dim, Integer lca, Dimensions dimensions) {
        Pair<Integer, Integer> startEnd = getDimension(dim, dimensions);

        //The dimension and LCA is converted to columns in the denormalized schema used by Dimensions
        if (lca == 0) {
            this.correlatedDimensions.put(startEnd._1, startEnd._2);
        } else if (lca < 0) {
            this.correlatedDimensions.put(startEnd._1, startEnd._2 + lca);
        } else {
            this.correlatedDimensions.put(startEnd._1, startEnd._1 + lca - 1);
        }
    }

    public void addScalingFactorForSource(String source, float scaling) {
        this.scalingFactorForSource.put(source, scaling);
    }

    public void addScalingFactorForMember(String dim, Integer level, String member, float scaling, Dimensions dimensions) {
        int column = getColumn(dim, level, dimensions);
        if ( ! this.scalingFactorForMember.containsKey(column)) {
            this.scalingFactorForMember.put(column, new HashMap<>());
        }
        HashMap<Object, Float> members = this.scalingFactorForMember.get(column);
        members.put(member, scaling);
    }

    /** Private Methods **/
    private int getColumn(String dim, Integer level, Dimensions dimensions) {
        Pair<Integer, Integer> startEnd = getDimension(dim, dimensions);

        //The dimension and level is converted to a column in the denormalized schema used by Dimensions
        int dimensionLevel = 0;
        if (level == 0) {
            dimensionLevel = startEnd._2;
        } else if (level < 0) {
            dimensionLevel = startEnd._2 + level;
        } else {
            dimensionLevel = startEnd._1 + level - 1;
        }

        //Verifies that the user has not specified a level that is greater than the number of levels in the dimension
        if (dimensionLevel < startEnd._1 || dimensionLevel > startEnd._2) {
            throw new IllegalArgumentException("CORE: the level \"" + level + "\" does not exist for dimension \"" + dim + "\"");
        }
        return dimensionLevel;
    }

    private Pair<Integer, Integer> getDimension(String dim, Dimensions dimensions) {
        HashMap<String, Pair<Integer, Integer>> dims = dimensions.getDimensions();
        Pair<Integer, Integer> startEnd = dims.get(dim);
        if (startEnd == null) {
            throw new IllegalArgumentException("CORE: the dimension \"" + dim + "\" does not exist");
        }
        return startEnd;
    }

    private boolean isCorrelatedBySources(TimeSeries[] groupOne, TimeSeries[] groupTwo) {
        if (this.correlatedSources.isEmpty()) {
            return true;
        }

        boolean correlated = true;
        for (TimeSeries ts : groupOne) {
            correlated &= this.correlatedSources.contains(ts.source);
        }

        for (TimeSeries ts : groupTwo) {
            correlated &= this.correlatedSources.contains(ts.source);
        }
        return correlated;
    }

    private boolean isCorrelatedByMembers(TimeSeries[] groupOne, TimeSeries[] groupTwo, Dimensions dimensions) {
        boolean correlated = true;
        for (HashMap.Entry<Integer, HashSet<Object>> columnToMembers : this.correlatedMembers.entrySet()) {
            for (TimeSeries ts : groupOne) {
                Object member = dimensions.get(ts.source)[columnToMembers.getKey()];
                correlated &= columnToMembers.getValue().contains(member);
            }

            for (TimeSeries ts : groupTwo) {
                Object member = dimensions.get(ts.source)[columnToMembers.getKey()];
                correlated &= columnToMembers.getValue().contains(member);
            }
        }
        return correlated;
    }

    private boolean isCorrelatedByLCALevels(TimeSeries[] groupOne, TimeSeries[] groupTwo, Dimensions dimensions) {
        return this.correlatedDimensions.isEmpty() || dimensions.correlatedByLCALevels(groupOne, groupTwo, this.correlatedDimensions);
    }

    private boolean isCorrelatedByDistance(TimeSeries[] groupOne, TimeSeries[] groupTwo, Dimensions dimensions) {
        return (this.distance == -1) || dimensions.correlatedByDistance(groupOne, groupTwo, this.distance);
    }

    /** Instance Variables **/
    private HashSet<String> correlatedSources;
    private HashMap<Integer, HashSet<Object>> correlatedMembers;
    private HashMap<Integer, Integer> correlatedDimensions;
    private float distance;

    private HashMap<String, Float> scalingFactorForSource;
    private HashMap<Integer, HashMap<Object, Float>> scalingFactorForMember;
}
