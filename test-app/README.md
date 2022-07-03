# Test Swim Application

A test Swim application to be used for verifying the Swim Kafka Connector 

## Getting Started

### Prerequisites

#### JDK versions

- Install JDK 11+
- Ensure that your `JAVA_HOME` environment variable points to the Java installation.
- Ensure that your `PATH` includes `$JAVA_HOME`.

### Running the Application 

#### On Windows

```bat
$ .\gradlew.bat run
```

#### On Linux or MacOS

```bash
$ ./gradlew run
```

This will start the Swim Plane ("Running Swim Plane ..." message should be displayed on the console). The Swim Plane
will listen on port 9001 for incoming requests. The port number and the other configurations for the Swim Plane is 
defined in the [src/main/resource/server.recon](src/main/resource/server.recon) file

### Creating a Docker Package

#### On Windows

```bash
$ .\gradlew.bat createDockerPackage
```

#### On Linux or MacOS

```bash
$ ./gradlew createDockerPackage
```

This will create a Docker package in the [build/Docker](build/Docker) folder.  


## Repository Structure

### Key files

- [gradlew](gradlew)/[gradlew.bat](gradlew.bat) — gradle wrapper
- [build.gradle](build.gradle) — gradle file to build and run the application
- [gradle.properties](gradle.properties) — application configuration variables

### Key directories

- [src](src) — application source code
    - [main/java](src/main/java) — java source code for the application
    - [main/resources](src/main/resources) — application configuration files
- [docker](docker) — support files for generating Docker images
- [gradle](gradle) — support files for the `gradlew` build script
