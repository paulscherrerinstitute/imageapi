# Readme

![Java CI](https://github.com/paulscherrerinstitute/imageapi/workflows/Java%20CI/badge.svg?branch=master)
[![codecov](https://codecov.io/gh/paulscherrerinstitute/imageapi/branch/master/graph/badge.svg)](https://codecov.io/gh/paulscherrerinstitute/imageapi)

## Requirements

* Java 11+

Older Java may work, but has not been tested.

## Build

Create a "fat" jar with all dependencies included:

```bash
gradle bootJar
```

## Publish to Bintray

```java
gradle bintrayPublish
```
