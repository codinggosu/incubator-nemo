/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.data;
import org.apache.nemo.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.UUID;
/**
 * A class to estimate the size of outputs generated by BlockOutputWriter.
 * needed for storing the elements while tracking their size, to acquireMemory form MemoryManager
 */
public class SizeTrackingVector implements Iterable {
  private static final Logger LOG = LoggerFactory.getLogger(SizeTrackingVector.class.getName());

  private ArrayList<Object> vector;
  // rate of nextSampleNum growth rate, grow nextSampleNum to prevent excessive sampling
  private double sampleGrowthRate = 1.1;
  private LinkedList<Pair<Long, Long>> samples;
  // number of updates/insertions since last update
  private long numUpdates;
  // average number of bytes between last two updates
  private long bytesPerUpdate;
  // value at which we will take our next sample
  private long nextSampleNum;
  private UUID uniqueID;


  public SizeTrackingVector() {
    this.vector = new ArrayList<Object>();
    this.samples = new LinkedList<Pair<Long, Long>>();
    this.resetSamples();
    this.uniqueID = UUID.randomUUID();
    LOG.info("SizeTrackingVector constructor called, unique ID is {} ", this.uniqueID);
  }

  /**
   * Implements iterator.
   * @return iterator of internal vector.
   */
  public Iterator iterator() {
    return this.vector.iterator();
  }

//  /**
//   * something.
//   */
//  public String getInfo() {
//    StringBuilder sb = new StringBuilder();
//    sb.append("numUpdates:");
//    sb.append(this.numUpdates);
//    sb.append("\n");
//    sb.append("nextSampleNum:");
//    sb.append(this.nextSampleNum);
//    sb.append("\n");
//    return sb.toString();
//  }

  private void resetSamples() {
    this.numUpdates = 1;
    this.nextSampleNum = 1;
    this.samples.clear();
    this.takeSample();
  }

  private void afterUpdate() {
    numUpdates += 1;
    if (nextSampleNum == numUpdates) {
      takeSample();
    }
  }

  /**
   * Append new item to vector.
   * @param element element to add to vector.
   */
  public void append(final Object element) {
    this.vector.add(element);
    LOG.info("append called, vector size {}", this.vector.size());
    LOG.info("size of object being appended {}", SizeEstimator.estimate(element));
    this.afterUpdate();
  }

  private void takeSample() {
    LOG.info("BEGIN, take sample, nextSampleNum {}, numUpdates {}", nextSampleNum, numUpdates);
    // add the current size of vector and the number of updates made to end of samples
    samples.add(Pair.of(SizeEstimator.estimate(this), numUpdates));
    if (samples.size() > 2) {
      samples.removeFirst();
    }
    // average change in bytes since last numUpdates
    long bytesDelta = (samples.getLast().left() - samples.getFirst().left())
      - (samples.getLast().right() - samples.getFirst().right());
    bytesPerUpdate = Math.max(0, bytesDelta);
    // grow nextSampleNum
    nextSampleNum = (long) Math.ceil(numUpdates * sampleGrowthRate);
    LOG.info("END take sample, nextSampleNum {}, numUpdates {}", nextSampleNum, numUpdates);
  }


  /**
   * Method to estimate the size of the vector by taking samples.
   * @return size in bytes
   */
  public long estimateSize() {
    LOG.info("estimateSize called: samples {}, unique ID {} ", this.samples, this.uniqueID);
    if (samples.isEmpty()) {
      return 0;
    }
    long currentlyTrackedSize = samples.getFirst().left();
    long mostRecentUpdate = bytesPerUpdate * (numUpdates - samples.getLast().right());
    return currentlyTrackedSize + mostRecentUpdate;
  }
}
