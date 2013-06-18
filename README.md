## Introduction

This is a simple lua mongo driver, work in progress.

## Building

Install lua-bson first.
https://github.com/cloudwu/lua-bson

```
make win
```
or
```
make linux
```

## Features

* connect to mongod
* runCommand
* insert document
* update document
* find return basic cursor, cursor hasNext() and next()
* findone

## Todo list

* write concern
* replica set
* gridFS
* tailable cursor
* more options for cursor
* more command
* and more ...

## Getting started

See test.lua
