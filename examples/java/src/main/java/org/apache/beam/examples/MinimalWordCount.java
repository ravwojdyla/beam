/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;


public class MinimalWordCount {

  public interface WordCountOptions extends DataflowPipelineOptions {}

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(WordCountOptions.class);

    Pipeline p = Pipeline.create(options);

    PCollection<String> p1 = p.apply(TextIO.Read.from("gs://apache-beam-samples/shakespeare/*"));
    PCollection<String> p2 = p.apply(TextIO.Read.from("gs://apache-beam-samples/shakespeare/*"));

    PCollection<String> union = PCollectionList.of(Lists.newArrayList(p1, p2)).apply(Flatten.<String>pCollections());

    union.apply(ParDo.of(new DoFn<String, String>() {
      private final Aggregator<Long, Long> counter = createAggregator("foo", Sum.ofLongs());

      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow w) {
        counter.addValue(1L);
        c.output(c.element());
      }
    }));

    p.run().waitUntilFinish();
  }
}
