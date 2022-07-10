# <a href="https://www.swimos.org"><img src="https://docs.swimos.org/readme/breach-marlin-blue-wide.svg"></a> Swim Kafka Connect Library&ensp;![version](https://img.shields.io/github/tag/swimos/swim.svg?label=version) [![license](https://img.shields.io/github/license/swimos/swim.svg?color=blue)](https://github.com/swimos/swim/blob/main/LICENSE)

## Introduction

This is a [Kafka Connect](https://docs.confluent.io/current/connect/index.html) library. The Swim Kafka Connect library 
acts as a bridge between Kafka  and Swim applications. This library should be configured to send messages generated from
Kafka topics to a specified Swim application.  This library should be used from within the Confluent  Platform (local or cloud).  

## Design and Implementation Details

### Swim Application Overview
Swim applications consist of interconnected, distributed objects, called Web Agents. Each Web Agent has URI address, like a REST endpoint. 
But unlike RESTful Web Services, Web Agents are stateful, and accessed via streaming APIs.  Each Web Agent has a set of named lanes, 
representing the properties and methods of the Web Agent. Lanes come in several varieties, corresponding to common data structures and access patterns.

To send a message to a specific Web Agent, external systems (like the Swim Kafka Connect Bridge) need the following:
1. Host URI : A Swim Application has a Host URI (similar to a web server's host URI).
2. Web Agent URI: Web Agents are hosted in a Swim Application and have unique URIs (similar to a REST endpoint).
3. Lane URI: The name of the lane in the Web Agent. 

For a detailed overview of Swim concepts please refer to [swimos.org](https://www.swimos.org/concepts/)

### Swim Kafka Connect Library Design
The Swim Kafka Connect library acts as a bridge between Kafka and Swim applications. The Swim Kafka Connect Library receives 
messages from the given Kafka topic and sends the message to a specified lane of a Web Agent running in a Swim application. 

In order to do so the above the Swim Kafka Connect Library does the following
- Parses the message from the kafka topic and extract the "id" value from the message. Parsing of Avro, Protocol Buffer and JSON 
  formats are supported.
- Constructs the Web Agent URI using the "id" extracted from the previous step and the Web Agent URI pattern specified as part of
  the Configuration. The "id" field maybe in the "key" part of the message or in the "value" part of the message. Support for
  can be configured to extracting the "id" field from either the "key" or the "value" part of the message. 
- Sends the message using the Host URI (specified in the Configuration), Web Agent URI (computed in the previous step) and 
  Lane URI (specified in the Configuration).

(Note: The Swim Kafka Connect library assumes that all messages from a given kafka topic is routed to the same type of Web Agent)


## Building from Source

### Choose JDK version

- Install JDK 11+ (11 or above)
- Ensure that your `JAVA_HOME` environment variable points to the Java installation.
- Ensure that your `PATH` includes `$JAVA_HOME`.

### Building the Application

#### On Windows

```bat
$ .\gradlew.bat build
```

#### On Linux or MacOS

```bash
$ ./gradlew build
```

This will run the unit tests and build the Swim Kafka Connect library.

### Creating the Swim Kafka Connect package

#### On Windows

```bat
$ .\gradlew.bat createKafkaConnectPackage
```

#### On Linux or MacOS

```bash
$ ./gradlew createKafkaConnectPackage
```
This will create the Swim Kafka Connect package in the [build/swim-kafka-connect](build/swim-kafka-connect) directory.

## Installation
Build and package the swim kafka connect library from source using the instructions above. Install the swim kafka connect
library in a local [Confluent Platform Installation](https://docs.confluent.io/current/quickstart/index.html).

## Swim Kafka Connector Configurations
Certain configuration properties have to be configured while instantiating the Swim Kafka Connector from the Confluent Platform.

### Kafka Configuration
The Swim Kafka Connector instance needs the standard Kafka configurations eg: topic, KeyDeserializer, ValueDeserializer etc.  

### Swim Application Configuration
The Swim Kafka Connect library needs the following Swim configurations.
1. **swim.host.uri**   
The Host URI of the Swim Application    
This needs to be of the form `warp://<host-server-name>:<port>` or `warps://<host-server-name>:<port>`. The `warp` prefix 
is needed if the Swim application is using unsecure web-sockets. The `warps` prefix is needed if the Swim application is using 
secure web-sockets.    
Eg For a swim application running on localhost on port 9001 using unsecure websockets, the value will be  
`warp://localhost:9001`


2. **swim.agent.uri.pattern**  
The Web Agent URI Pattern   
This needs to match the agent URI pattern defined in the `server.recon` configuration file of the Swim application. This is 
typically of the form `/<path1>/:id` or `/<path1>/<path2>:id` where `<path1>` and `<path2>` are string literals and `:id` is
the variable part of the URI pattern.  
Eg: `/agent/:id` or `/user/:id` etc.


3. **swim.lane.uri**  
The Lane URI of the Web Agent    
This needs to match the lane URI defined in the Web Agent. This is typically of the form `<lanePath1>` or `<lanePath1>/<lanePath2>`
where `<lanePath1>` and `<lanePath2>` are string literals.  
Eg: `abc` or `latest` etc.

   
4. **swim.use.value.field.for.agent.id**  
Whether to use the key field or value field of the Sink Record to extract the Id
This is a boolean property. If this property is set to `true` then the Swim Kafka Connect library will use the value field of the message
to extract the `id` which will be used to compute the Web Agent's URI. If this property is set to `false` then the Swim Kafka Connect 
library will use the key field of the message.


5. **swim.agent.id.extractor**  
Recon selector expression to parse the id  
The selector expression that is needed to extract the appropriate field from the key/value part of the message.
If the key part of the message is used to parse the id (Refer to the **swim.use.value.field.for.agent.id** property) then
this property can be left as empty.  
If the value part of the message is used to parse the id then the selector expression needs to match the field name of the 
message payload that has the id value. 
Eg: If the value is a JSON structure and `id` value is associated with a json key called `userId` then the expression would be:
`$userId`  
Refer to the [AgentUriParserSpec](https://github.com/swimos/swim-kafka-connect/blob/main/src/test/java/swim/kafka/connector/sink/id/AgentUriParserSpec.java)
for more examples.


## Validating with the Test Application
The Swim Kafka Connect library can be validated by sending data to the test application in the [test-app/](test-app) directory. This can 
be done using the following steps

### Run the Test Application
The Test Application can be run from the Commandline or using a pre-built Docker Image. Using either of these methods, the
application will start up and bind to port 9001.

#### Use Commandline
Go to the [test-app/](test-app) directory and execute the following command on the commandline.  
For Linux/MacOS: `./gradlew run`  
For Windows: `.\gradlew.bat run`  

#### Use Docker Image
A docker image with the test-application is available [here](https://github.com/orgs/swimos/packages/container/package/swim-kafka-connector-test-app).  
Pull the docker image using the following command: `docker pull ghcr.io/swimos/swim-kafka-connector-test-app:latest`  
Run the application using the following command: `docker run -p 9001:9001 ghcr.io/swimos/swim-kafka-connector-test-app:latest`

### Configure Connector 
Instantiate the Swim Kafka Connector in the Confluent platform with the appropriate Kafka Configurations and the following 
Swim Application Configurations based on the Test Application's configuration.
1. **The Host URI of the Swim Application**: This value should be `warp://localhost:9001` if the Test Application is running on the same host as 
the Confluent Platform. If not, then replace the "localhost" with the Fully Qualified Domain Name of the machine.
of the machine which is running the Test Application.  


2. **The Web Agent URI Pattern**: This value should be `/agent/:id` since this is the agent uri pattern specified in the [server.recon](test-app/src/main/resources/server.recon#L5)
file of the Test Application.  


3. **The Lane URI of the Web Agent**: This value should be `latest` since this is the lane name specified in the [LatestValue.java](test-app/src/main/java/swim/app/LatestValue.java#32) 
file of the Test Application. The `LatestValue.java` is the class that is associated with the agent as specified in the [server.recon](test-app/src/main/resources/server.recon#L6).  


4. **Whether to use the key field or value field of the Sink Record to extract the Id**: If messages generated in the kafka topic 
uses a key and if the key corresponds to the id of the object associated with the message then set this value to `false`, otherwise
set this value to `true`  


5. **Recon selector expression to parse the id**:  If the previous property is set to `true` then this field can be left empty.
If the previous property is set to false then use the appropriate selector expression. The selector expression needs to match the 
field name of the message payload that has the id value.  
Eg: If the value is a JSON structure and `id` value is associated with a json key called `userId` then the expression would be: `$userId`    
Refer to the [AgentUriParserSpec](https://github.com/swimos/swim-kafka-connect/blob/main/src/test/java/swim/kafka/connector/sink/id/AgentUriParserSpec.java) 
for more examples.


### Access the data from the Application  
Data sent to the Web Agents in the Test Application from the Swim Kafka Connect library can be accessed using HTTP APIs.
For example, if the `id` extracted from the message is "User_5" then the Swim Kafka Connect Library will compute the agent URI to
be: `/agent/User_5`. The HTTP API to get the message from this agent will be  
`http://<host-name>:9001/agent/User_5?lane=latestData`

If the `id` extracted from the message is "User_7", then the HTTP API to get the message from this agent will be:  
`http://<host-name>:9001/agent/User_7?lane=latestData`

Here the `<host-name>` will be "localhost" if the Test Application is running on the same machine as the Swim Kafka Connect library.
If the Test Application is running on a different machine then the `<host-name>` will be the Fully Qualified Domain Name of the machine.

## Repository Structure

### Key files

- [gradlew](gradlew)/[gradlew.bat](gradlew.bat) — gradle wrapper
- [build.gradle](build.gradle) — gradle file to build the library
- [gradle.properties](gradle.properties) — configuration variables

### Key directories

- [src](src) — source code
    - [main/java](src/main/java) — java source code
    - [main/resources](src/main/resources) — configuration files
    - [test](src/test)- unit test code
- [package](package) — files related to the swim-kafka-connect package
    - [assets](package/assets) — the assets associated with the package
    - [doc](package/doc) — the docs associated with the package
    - [etc](package/etc) — sample configurations with the package
- [test-app](test-app) — test Swim application which maybe used to validate the swim-kafka-connect library