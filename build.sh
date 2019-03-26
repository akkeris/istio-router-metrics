#!/bin/sh

cd /go/src
go get "github.com/bsm/sarama-cluster"
go get "gopkg.in/Shopify/sarama.v1"
cd /go/src/router-metrics
go build router-metrics.go


