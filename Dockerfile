FROM golang:1.23-bullseye as build

WORKDIR /go/src/indexing-service

COPY go.* .
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 go build -o /go/bin/indexing-service ./cmd

FROM gcr.io/distroless/static-debian12
COPY --from=build /go/bin/indexing-service /usr/bin/

ENTRYPOINT ["/usr/bin/indexing-service"]