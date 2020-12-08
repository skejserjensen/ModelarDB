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

import java.util.ArrayList;
import java.util.HashMap;

public class Dimensions {

    /** Constructors **/
    public Dimensions(String[] dimensions) {
        ArrayList<String> names = new ArrayList<>();
        HashMap<String, Pair<Integer, Integer>> dims = new HashMap<>();
        ArrayList<Double> weights = new ArrayList<>();
        ArrayList<String> columns = new ArrayList<>();
        ArrayList<Dimensions.Types> types = new ArrayList<>();

        int startIndex = 0;
        for (String dimension : dimensions) {
            String[] split = dimension.trim().split(",");

            //The last element may be a weight but it is not guaranteed, however, as all members
            // must have an associated type only the name and weight consist of a single element
            int numberOfColumns;
            if ( ! split[split.length - 1].trim().contains(" ")) {
                //The weight is stored as 1/W so a larger weight is better
                weights.add(1.0 / Double.parseDouble(split[split.length - 1]));
                numberOfColumns = split.length - 1;
            } else {
                weights.add(1.0);
                numberOfColumns = split.length;
            }

            //The columns are prefixed by the dimension's name which should not be included
            String name = split[0].trim();
            names.add(name);
            dims.put(name, new Pair<>(startIndex, startIndex += (numberOfColumns - 2)));
            startIndex += 1;
            for (int i = 1; i < numberOfColumns; i++) {
                String[] column = split[i].trim().split(" ");
                if (column.length != 2) {
                    throw new IllegalArgumentException("CORE: unparsable dimension \"" + dimension + "\"");
                }
                columns.add(column[0].trim());
                types.add(parseTypes(column[1].trim()));
            }
        }

        this.dims = dims;
        this.names = names.toArray(new String[0]);
        this.weights = weights.stream().mapToDouble(w -> w).toArray();
        this.columns = columns.toArray(new String[0]);
        this.types = types.toArray(new Dimensions.Types[0]);
        this.rows = new HashMap<>();
        this.emptyRow = new Object[0];
    }

    /** Public Methods **/
    public void add(String line) {
        add(line.split(","));
    }

    public Object[] get(String source) {
        Object[] dims = this.rows.getOrDefault(source, this.emptyRow);
        if (dims == this.emptyRow && this.columns.length != 0) {
            throw new IllegalArgumentException("CORE: no dimensions defined for " + source);
        }
        return dims;
    }

    public String[] getSources() {
        return this.rows.keySet().toArray(new String[0]);
    }

    public HashMap<String, Pair<Integer, Integer>> getDimensions() {
        return this.dims;
    }

    public String[] getColumns() {
        return this.columns;
    }

    public Types[] getTypes() {
        return this.types;
    }

    public String getSchema() {
        if (this.types.length == 0) {
            return "";
        }

        //The schema is build with a starting comma so it is easy to embed into a CREATE table statement
        StringBuilder sb = new StringBuilder();
        sb.append(", ");
        int withPunctuation = this.columns.length - 1;
        for (int i = 0; i <  withPunctuation; i++) {
            sb.append(this.columns[i]);
            sb.append(' ');
            sb.append(this.types[i].toString());
            sb.append(", ");
        }
        sb.append(this.columns[withPunctuation]);
        sb.append(' ');
        sb.append(this.types[withPunctuation].toString());
        return sb.toString();
    }

    public float getLowestNoneZeroDistance() {
        float highestLevelCount = this.dims.entrySet().stream().mapToInt(dim ->
                (dim.getValue()._2 - dim.getValue()._1 + 1)).max().getAsInt();
        return (float) (1.0 / highestLevelCount / this.names.length);
    }

    public boolean correlatedByLCALevels(TimeSeries[] tsgA, TimeSeries[] tsgB, HashMap<Integer, Integer> correlation) {
        //Ensures that dimensions are available before grouping
        if (this.names == null) {
            throw new IllegalArgumentException("CORE: grouping by dimensions requires a dimensions file");
        }

        boolean correlated = true;
        for (HashMap.Entry<Integer, Integer> entry : correlation.entrySet()) {
            int dimensionStart = entry.getKey();
            int correlationEnd = entry.getValue();
            int lca = this.getLowestCommonAncestorLevel(dimensionStart, correlationEnd, tsgA, tsgB);

            //The LCA level is computed starting from 0, and 1 is added for each level where the two groups share members
            correlated &= lca > (correlationEnd - dimensionStart);
        }
        return correlated;
    }

    public boolean correlatedByDistance(TimeSeries[] tsgA, TimeSeries[] tsgB, float distanceBound) {
        //Ensures that dimensions are available before grouping
        if (this.names == null) {
            throw new IllegalArgumentException("CORE: grouping by dimensions require a dimensions file");
        }

        float distance = 0;
        for (int i = 0; i < this.names.length; i++) {
            String dimension = this.names[i];
            Pair<Integer, Integer> se = this.dims.get(dimension);

            //The LCA level is computed starting from 0, and 1 is added for each level where the two groups share members
            int lca = this.getLowestCommonAncestorLevel(se._1, se._2, tsgA, tsgB);

            //The distance between dimensions is computed as: (length - LCA level) / length
            double length = se._2 - se._1 + 1.0;
            double normalized = (length - lca) / length;
            double weight = this.weights[i];
            distance += weight * normalized;
        }
        return Math.min(distance / this.names.length, 1.0) <= distanceBound;
    }

    public void add(String[] row) {
        //A row consists of [Source, Members+]
        if (this.columns.length + 1 != row.length) {
            throw new IllegalArgumentException("CORE: each source must include members for all dimensions");
        }
        this.rows.put(row[0], parseRow(row));
    }

    public Object parse(int index, String member) {
        try {
            switch (this.types[index]) {
                case INT:
                    return Integer.parseInt(member);
                case LONG:
                    return Long.parseLong(member);
                case FLOAT:
                    return Float.parseFloat(member);
                case DOUBLE:
                    return Double.parseDouble(member);
                case TEXT:
                    return member;
                default:
                    throw new IllegalArgumentException("CORE: \"" + this.types[index] + "\" is not supported");
            }
        }
        catch (NumberFormatException nfe){
            throw new IllegalArgumentException("CORE: \"" + member+ "\" is not a \"" + this.types[index] + "\"");
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        //Indents the header to match the first row
        int indent = 0;
        if  ( ! this.rows.isEmpty()) {
            indent = this.rows.keySet().iterator().next().length() + 3;
        }
        for (int i = 0; i < indent; i++) {
            sb.append(' ');
        }

        //Adds the header
        int withComma = this.columns.length - 1;
        for (int i = 0; i < withComma; i++) {
            sb.append(this.columns[i]);
            sb.append(' ');
            sb.append(this.types[i]);
            sb.append(", ");
        }
        sb.append(this.columns[withComma]);
        sb.append(' ');
        sb.append(this.types[withComma]);

        //Adds the rows
        for (HashMap.Entry<String, Object[]> row : this.rows.entrySet()) {
            sb.append("\n ");
            sb.append(row.getKey());
            sb.append(", ");
            for (int i = 0; i < withComma; i++) {
                sb.append(row.getValue()[i]);
                sb.append(", ");
            }
            sb.append(row.getValue()[withComma]);
        }
        return sb.toString();
    }

    /** Private Methods **/
    private Dimensions.Types parseTypes(String type) {
        switch (type.trim().toLowerCase()) {
            case "int":
            case "integer":
                return Types.INT;
            case "long":
                return Types.LONG;
            case "float":
                return Types.FLOAT;
            case "double":
                return Types.DOUBLE;
            case "string":
            case "text":
                return Types.TEXT;
            default:
                throw new IllegalArgumentException("CORE: the type \"" + type + "\" is not supported");
        }
    }

    private Object[] parseRow(String[] row) {
        //A row consists of [Source, Members+]
        int length = row.length - 1;
        String line = "";
        Object[] parsed = new Object[length];
        for (int i = 0; i < length; i++) {
            try {
                line = row[i + 1].trim();
                switch (this.types[i]) {
                    case INT:
                        parsed[i] = Integer.parseInt(line);
                        break;
                    case LONG:
                        parsed[i] = Long.parseLong(line);
                        break;
                    case FLOAT:
                        parsed[i] = Float.parseFloat(line);
                        break;
                    case DOUBLE:
                        parsed[i] = Double.parseDouble(line);
                        break;
                    case TEXT:
                        parsed[i] = line;
                        break;
                }
            }
            catch (NumberFormatException nfe){
                throw new IllegalArgumentException("CORE: \"" + line + "\" is not a \"" + this.types[i] + "\"");
            }
        }
        return parsed;
    }

    private int getLowestCommonAncestorLevel(int start, int end, TimeSeries[] tsgA, TimeSeries[] tsgB) {
        //Members are ordered by their level in their respective dimension
        try {
            int lca = 0;
            for (int index = start; index <= end; index++) {
                //All time series in the group must have the same member for each level
                Object match = this.rows.get(tsgA[0].source)[index];

                for (TimeSeries ts : tsgA) {
                    Object value = this.rows.get(ts.source)[index];
                    if ( ! match.equals(value)) {
                        return lca;
                    }
                }

                for (TimeSeries ts : tsgB) {
                    Object value = this.rows.get(ts.source)[index];
                    if ( ! match.equals(value)) {
                        return lca;
                    }
                }
                lca += 1;
            }
            return lca;

        } catch (NullPointerException npe) {
            throw new UnsupportedOperationException("CORE: a source is not included as part of the dimensions file");
        } catch (ArrayIndexOutOfBoundsException oube) {
            throw new UnsupportedOperationException("CORE: all dimensional values are not specified for a source");
        }
    }

    /** Instance Variables **/
    private String[] names;
    private HashMap<String, Pair<Integer, Integer>> dims;
    private Object[] emptyRow;
    private double[] weights;
    private String[] columns;
    private Dimensions.Types[] types;
    private HashMap<String, Object[]> rows;
    public enum Types {INT, LONG, FLOAT, DOUBLE, TEXT}
}
