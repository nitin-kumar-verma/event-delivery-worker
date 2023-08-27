FROM golang:latest as build

WORKDIR /

COPY ./go.mod .
COPY ./go.sum .

RUN go mod download

FROM build

COPY ./ .

RUN go build -o /build

EXPOSE 3001
ENTRYPOINT [ "/build" ]
