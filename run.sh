#!/bin/sh

BASEDIR=$(dirname "$0")

cd $BASEDIR


sbt clean compile run -J-Xms4G -J-Xmx16G
