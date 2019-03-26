#!/bin/sh

cd /go/src
go get "github.com/bsm/sarama-cluster"
cd /go/src/router-metrics
go build router-metrics.go

