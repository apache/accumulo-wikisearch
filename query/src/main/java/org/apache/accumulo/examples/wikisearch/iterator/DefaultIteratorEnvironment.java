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
package org.apache.accumulo.examples.wikisearch.iterator;

import java.io.IOException;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MapFileIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class DefaultIteratorEnvironment implements IteratorEnvironment {

  AccumuloConfiguration conf;

  public DefaultIteratorEnvironment() {
    this.conf = DefaultConfiguration.getInstance();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName)
      throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    return new MapFileIterator(fs, mapFileName, conf);
  }

  @Override
  public AccumuloConfiguration getConfig() {
    return conf;
  }

  @Override
  public boolean isSamplingEnabled() {
    return false;
  }

  @Override
  public IteratorScope getIteratorScope() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isFullMajorCompaction() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Authorizations getAuthorizations() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SamplerConfiguration getSamplerConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public IteratorEnvironment cloneWithSamplingEnabled() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isUserCompaction() {
    return false;
  }
}
