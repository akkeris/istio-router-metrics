FROM golang:1.10
RUN apt-get update
RUN apt-get install -y tzdata git
RUN cp /usr/share/zoneinfo/America/Denver /etc/localtime
RUN mkdir -p /go/src/router-metrics
ADD router-metrics.go  /go/src/router-metrics/router-metrics.go
ADD build.sh /build.sh
RUN chmod +x /build.sh
RUN /build.sh
CMD ["/go/src/router-metrics/router-metrics"]







