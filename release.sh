#!/bin/bash

mvn -B com.atlassian.maven.plugins:maven-jgitflow-plugin:release-start
mvn -B com.atlassian.maven.plugins:maven-jgitflow-plugin:release-finish
