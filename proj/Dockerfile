FROM golang:1.15-alpine as builder

RUN apk add --no-cache make gcc musl-dev linux-headers git

WORKDIR /app

ADD . .
RUN go build .
