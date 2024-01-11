/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CalculateAverage_ktm {

    private static final String FILE = "./measurements.txt";

    private static class Measurement {
        double min;
        double max;
        double sum;
        long count;

        public Measurement(Double value) {
            min = value;
            max = min;
            sum = min;
            count = 1;
        }

        public Measurement() {
            min = 0;
            max = min;
            sum = min;
            count = 0;
        }

        public void add(Double d) {
            ++count;
            if (d < min)
                min = d;
            if (d > max)
                max = d;
            sum += d;
        }

        public Measurement merge(Measurement m1) {
            if (m1.min < this.min)
                this.min = m1.min;
            if (m1.max > this.max)
                this.max = m1.max;
            this.sum += m1.sum;
            this.count += m1.count;
            return this;
        }

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {
        // long before = System.currentTimeMillis();

        ConcurrentHashMap<String, Measurement> rMap = new ConcurrentHashMap<String, Measurement>(10);
        Files.newBufferedReader(Path.of(FILE)).lines().parallel()
                .forEach(record -> {
                    int delim = record.indexOf(";");
                    String key = record.substring(0, delim);
                    Double d = Double.parseDouble(record.substring(delim + 1));
                    Measurement measured = rMap.get(key);
                    if (measured == null)
                        measured = new Measurement();

                    measured.add(d);

                    rMap.put(key, measured);
                });

        System.out.print("{");
        System.out.print(rMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Object::toString)
                .collect(Collectors.joining(", ")));
        System.out.println("}");

        // System.out.println("Took: " + (System.currentTimeMillis() - before));
    }
}
