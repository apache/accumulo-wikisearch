<?xml version="1.0"?>
<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<configuration>
  <property>
    <name>wikipedia.accumulo.zookeepers</name>
    <value>localhost:2181</value>
  </property>
  <property>
    <name>wikipedia.accumulo.instance_name</name>
    <value>uno</value>
  </property>
  <property>
    <name>wikipedia.accumulo.user</name>
    <value>root</value>
  </property>
  <property>
    <name>wikipedia.accumulo.password</name>
    <value>secret</value>
  </property>
  <property>
    <name>wikipedia.accumulo.table</name>
    <value>wikipedia</value>
  </property>
  <property>
    <name>wikipedia.ingest.partitions</name>
    <value>1</value>
  </property>
</configuration>
