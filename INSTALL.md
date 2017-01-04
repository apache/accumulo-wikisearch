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
# Wikisearch Installation

Instructions for installing and running the Accumulo Wikisearch example.

## Ingest
 
### Prerequisites

1. Accumulo, Hadoop, and ZooKeeper must be installed and running
1. Download one or more [wikipedia dump files][dump-files] and put them in an HDFS directory.
	 You will want to grab the files with the link name of pages-articles.xml.bz2. Though not strictly
	 required, the ingest will go more quickly if the files are decompressed:

        $ bunzip2 enwiki-*-pages-articles.xml.bz2
        $ hadoop fs -put enwiki-*-pages-articles.xml /wikipedia/enwiki-pages-articles.xml

### Instructions
	
1. Create a `wikipedia.xml` file (or `wikipedia_parallel.xml` if running parallel version) from
   [wikipedia.xml.example] or [wikipedia_parallel.xml.example] and modify for your Accumulo
   installation.
   
        $ cp ingest/conf
        $ cp wikipedia.xml.example wikipedia.xml
        $ vim wikipedia.xml
 
1. Copy `ingest/lib/wikisearch-*.jar` to `$ACCUMULO_HOME/lib/ext`
1. Run `ingest/bin/ingest.sh` (or `ingest_parallel.sh` if running parallel version) with one
   argument (the name of the directory in HDFS where the wikipedia XML files reside) and this will
   kick off a MapReduce job to ingest the data into Accumulo.

## Query
 
### Prerequisites

1. The query software was tested using JBoss AS 6. Install the JBoss distro and follow the instructions below
   to build the EJB jar and WAR file required.
  * To stop the JBoss warnings about WSDescriptorDeployer and JMSDescriptorDeployer, these deployers can be
    removed from `$JBOSS_HOME/server/default/deployers/jbossws.deployer/META-INF/stack-agnostic-jboss-beans.xml`
1. Ensure that you have successfully run `mvn clean install` at the Wikisearch top level to install the jars
   into your local maven repo before building the query package.
	
### Instructions

1. Create a `ejb-jar.xml` from [ejb-jar.xml.example] and modify it to contain the same information
   that you put into `wikipedia.xml` in the ingest steps above:

        cd query/src/main/resources/META-INF/
        cp ejb-jar.xml.example ejb-jar.xml
        vim ejb-jar.xml

1. Re-build the query distribution by running `mvn package assembly:single` in the query module's directory.
1. Untar the resulting file in the `$JBOSS_HOME/server/default` directory.

        $ cd $JBOSS_HOME/server/default
        $ tar -xzf /some/path/to/wikisearch/query/target/wikisearch-query*.tar.gz
 
   This will place the dependent jars in the lib directory and the EJB jar into the deploy directory.
1. Next, copy the wikisearch*.war file in the query-war/target directory to $JBOSS_HOME/server/default/deploy. 
1. Start JBoss ($JBOSS_HOME/bin/run.sh)
1. Use the Accumulo shell and give the user permissions for the wikis that you loaded:
			
        > setauths -u <user> -s all,enwiki,eswiki,frwiki,fawiki
			  
1. Copy the following jars to the `$ACCUMULO_HOME/lib/ext` directory from the `$JBOSS_HOME/server/default/lib` directory:
	
        kryo*.jar
        minlog*.jar
        commons-jexl*.jar
		
1. Copy `$JBOSS_HOME/server/default/deploy/wikisearch-query*.jar` to `$ACCUMULO_HOME/lib/ext.`

1. At this point you should be able to open a browser and view the page:

        http://localhost:8080/accumulo-wikisearch/ui.html

  You can issue the queries using this user interface or via the following REST urls:

        <host>/accumulo-wikisearch/rest/Query/xml
        <host>/accumulo-wikisearch/rest/Query/html
        <host>/accumulo-wikisearch/rest/Query/yaml
        <host>/accumulo-wikisearch/rest/Query/json.

  There are two parameters to the REST service, query and auths. The query parameter is the same string that you would type
	into the search box at ui.jsp, and the auths parameter is a comma-separated list of wikis that you want to search (i.e.
	enwiki,frwiki,dewiki, etc. Or you can use all) 
	
  - NOTE: Ran into a [bug] that did not allow an EJB3.1 war file. The workaround is to separate the RESTEasy servlet
    from the EJBs by creating an EJB jar and a WAR file.

[ejb-jar.xml.example]: query/src/main/resources/META-INF/ejb-jar.xml.example
[dump-files]: http://dumps.wikimedia.org/backup-index.html
[wikipedia.xml.example]: ingest/conf/wikipedia.xml.example
[wikipedia_parallel.xml.example]: ingest/conf/wikipedia_parallel.xml.example
[bug]: https://issues.jboss.org/browse/RESTEASY-531
