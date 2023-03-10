FROM golang:alpine AS builder

LABEL stage=gobuilder

ENV CGO_ENABLED 0
ENV GOOS linux

WORKDIR /build

COPY . .
RUN go mod download

RUN go build -o sui-indexer ./main.go


FROM alpine

RUN apk update --no-cache && apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /build/sui-indexer /app/sui-indexer

CMD ["./sui-indexer", "-f", "etc/sui-indexer.yaml", "-logtostderr", "-v=8"]