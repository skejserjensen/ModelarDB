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
package dk.aau.modelardb.core.models;

import java.util.Arrays;
import java.lang.reflect.Constructor;

public class ModelFactory {

    /** Public Methods **/
    public static Model getFallbackModel(float error, int limit) {
            return getModels(new String[]{"dk.aau.modelardb.core.models.UncompressedModel"}, error, limit)[0];
    }

    public static Model[] getModels(String[] modelNames, float error, int limit) {
        return Arrays.stream(modelNames).map(sm -> getModel(sm, error, limit)).toArray(size -> new Model[size]);
    }

    public static Model getModel(String modelName, float error, int limit) {
        try {
            Constructor constructor = Class.forName(modelName).getDeclaredConstructor(float.class, int.class);
            constructor.setAccessible(true);
            return (Model) constructor.newInstance(error, limit);
        } catch (ClassNotFoundException e) {
            throw new UnsupportedOperationException("model \"" + modelName + "\" could not be found in CLASSPATH");
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException("model \"" + modelName + "\" require a \"error: float, limit: int\" constructor");
        } catch (Exception e) {
            throw new UnsupportedOperationException("construction of \"" + modelName + "\" is not possible");
        }
    }
}
