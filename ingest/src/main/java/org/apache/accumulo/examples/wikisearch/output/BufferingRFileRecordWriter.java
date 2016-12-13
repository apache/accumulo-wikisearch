/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.examples.wikisearch.output;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.examples.wikisearch.ingest.WikipediaConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

final class BufferingRFileRecordWriter extends RecordWriter<Text,Mutation> {
  private final long maxSize;
  private final Configuration conf;
  private long size;
  
  private Map<Text,TreeMap<Key,Value>> buffers = new HashMap<Text,TreeMap<Key,Value>>();
  private Map<Text,Long> bufferSizes = new HashMap<Text,Long>();
  
  private TreeMap<Key,Value> getBuffer(Text tablename) {
    TreeMap<Key,Value> buffer = buffers.get(tablename);
    if (buffer == null) {
      buffer = new TreeMap<Key,Value>();
      buffers.put(tablename, buffer);
      bufferSizes.put(tablename, 0l);
    }
    return buffer;
  }
  
  private Text getLargestTablename() {
    long max = 0;
    Text table = null;
    for (Entry<Text,Long> e : bufferSizes.entrySet()) {
      if (e.getValue() > max) {
        max = e.getValue();
        table = e.getKey();
      }
    }
    return table;
  }
  
  private void flushLargestTable() throws IOException {
    Text tablename = getLargestTablename();
    if (tablename == null)
      return;
    long bufferSize = bufferSizes.get(tablename);
    TreeMap<Key,Value> buffer = buffers.get(tablename);
    if (buffer.size() == 0)
      return;
    
    Connector conn;
	try {		
	  conn = WikipediaConfiguration.getConnector(conf);
      BatchWriterConfig bwconfig = new BatchWriterConfig();
      BatchWriter writer = conn.createBatchWriter(tablename.toString(), bwconfig);
      for (Entry<Key,Value> e : buffer.entrySet()) {
        Key k = e.getKey();
    	Mutation m = new Mutation();
    	m.put(k.getColumnFamily(), k.getColumnQualifier(), e.getValue());
        writer.addMutation(m);
      }
      writer.close();
	} catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
	  System.err.println("Error occured in flushLargestTable: " + e.getMessage());
	  e.printStackTrace();
	}    
    // TODO get the table configuration for the given table?
    
    size -= bufferSize;
    buffer.clear();
    bufferSizes.put(tablename, 0l);
  }
  
  BufferingRFileRecordWriter(long maxSize, Configuration conf) {
    this.maxSize = maxSize;
    this.conf = conf;
  }
  
  @Override
  public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
    while (size > 0)
      flushLargestTable();
  }
  
  @Override
  public void write(Text table, Mutation mutation) throws IOException, InterruptedException {
    TreeMap<Key,Value> buffer = getBuffer(table);
    int mutationSize = 0;
    for (ColumnUpdate update : mutation.getUpdates()) {
      Key k = new Key(mutation.getRow(), update.getColumnFamily(), update.getColumnQualifier(), update.getColumnVisibility(), update.getTimestamp(),
          update.isDeleted());
      Value v = new Value(update.getValue());
      // TODO account for object overhead
      mutationSize += k.getSize();
      mutationSize += v.getSize();
      buffer.put(k, v);
    }
    size += mutationSize;
    long bufferSize = bufferSizes.get(table);
    
    // TODO use a MutableLong instead
    bufferSize += mutationSize;
    bufferSizes.put(table, bufferSize);
    
    while (size >= maxSize) {
      flushLargestTable();
    }
  }
  
}
