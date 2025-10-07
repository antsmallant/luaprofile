#!/bin/bash

./build/lua-5.4.8/install/bin/lua example_sample.lua

~/software/FlameGraph/flamegraph.pl cpu-samples.txt > cpu-samples.svg