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
package dk.aau.modelardb.core.models;

import java.lang.reflect.Constructor;
import java.util.stream.IntStream;

public class ModelFactory {

    /** Public Methods **/
    public static Model getFallbackModel(float error, int limit) {
        //Per definition the mid of the fallback model is one
        return getModel("dk.aau.modelardb.core.models.UncompressedModel", 1, error, limit);
    }

    public static Model[] getModels(String[] models, int[] mids, float error, int limit) {
        for (String modelName : models) {
            //The fallback model is purposely designed without any limits to ensure that progress can always be made
            if (modelName.equals("dk.aau.modelardb.core.models.UncompressedModel")) {
                throw new IllegalArgumentException("CORE: the fallback model cannot be used as a normal model");
            }
        }
        return IntStream.range(0, models.length).mapToObj(i-> getModel(models[i], mids[i], error, limit)).toArray(Model[]::new);
    }

    public static Model getModel(String modelName, int mid, float error, int limit) {
        try {
            Constructor constructor = Class.forName(modelName).getDeclaredConstructor(int.class, float.class, int.class);
            constructor.setAccessible(true);
            return (Model) constructor.newInstance(mid, error, limit);
        } catch (ClassNotFoundException e) {
            throw new UnsupportedOperationException("CORE: model \"" + modelName + "\" could not be found in CLASSPATH");
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException("CORE: model \"" + modelName + "\" require a \"error: float, limit: int\" constructor");
        } catch (Exception e) {
            throw new UnsupportedOperationException("CORE: construction of \"" + modelName + "\" is not possible");
        }
    }
}
