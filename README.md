# <a href="https://www.swimos.org"><img src="https://docs.swimos.org/readme/breach-marlin-blue-wide.svg"></a> Swim Kafka Connect Library&ensp;![version](https://img.shields.io/github/tag/swimos/swim.svg?label=version) [![license](https://img.shields.io/github/license/swimos/swim.svg?color=blue)](https://github.com/swimos/swim/blob/main/LICENSE)

This is a [Kafka Connect](https://docs.confluent.io/current/connect/index.html) library. It acts as a bridge between Kafka
and Swim applications. This library can be configured to send messages generated from Kafka topics to a specified Swim application.
This library  be used from within the Confluent  Platform (local or cloud).  

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

```bat
$ .\gradlew.bat createKafkaConnectPackage
```

#### On Linux or MacOS

```bash
$ ./gradlew createKafkaConnectPackage
```
This will create the Swim Kafka Connect package in the [build/swim-kafka-connect](build/swim-kafka-connect) directory.

## Usage
Build and package the swim kafka connect library from source using the instructions above. Install the swim kafka connect
library in a local [Confluent Platform Installation](https://docs.confluent.io/current/quickstart/index.html). 

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