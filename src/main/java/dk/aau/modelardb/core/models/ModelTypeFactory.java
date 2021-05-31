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
package dk.aau.modelardb.core.models;

import java.lang.reflect.Constructor;
import java.util.stream.IntStream;

public class ModelTypeFactory {

    /** Public Methods **/
    public static ModelType getFallbackModelType(float errorBound, int lengthBound) {
        //Per definition the mtid of the fallback model is one
        return getModel("dk.aau.modelardb.core.models.UncompressedModelType", 1, errorBound, lengthBound);
    }

    public static ModelType[] getModelTypes(String[] modelTypes, int[] mtids, float errorBound, int lengthBound) {
        for (String modelTypeName : modelTypes) {
            //The fallback model type is purposely designed without any limits to ensure that progress can always be made
            if (modelTypeName.equals("dk.aau.modelardb.core.models.UncompressedModelType")) {
                throw new IllegalArgumentException("CORE: the fallback model type cannot be used as a normal model type");
            }
        }
        return IntStream.range(0, modelTypes.length).mapToObj(i ->
                getModel(modelTypes[i], mtids[i], errorBound, lengthBound)).toArray(ModelType[]::new);
    }

    public static ModelType getModel(String modelTypeName, int mtid, float error, int limit) {
        try {
            Constructor<?> constructor = Class.forName(modelTypeName).getDeclaredConstructor(int.class, float.class, int.class);
            constructor.setAccessible(true);
            return (ModelType) constructor.newInstance(mtid, error, limit);
        } catch (ClassNotFoundException e) {
            throw new UnsupportedOperationException("CORE: model type \"" + modelTypeName + "\" could not be found in CLASSPATH", e);
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException("CORE: model type \"" + modelTypeName + "\" require a \"int, float, int\" constructor", e);
        } catch (Exception e) {
            throw new UnsupportedOperationException("CORE: construction of \"" + modelTypeName + "\" is not possible", e);
        }
    }
}
