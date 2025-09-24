#!/bin/bash

curdir=$(pwd)
rm -rf build
mkdir -p build

cd $curdir/build
git clone https://github.com/antsmallant/lua-5.4.8.git
cd lua-5.4.8
make linux
make local

cd $curdir
make clean
make linux





