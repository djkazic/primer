FROM golang:1.20

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -v -o /usr/local/bin/app ./...
RUN apt-get update && apt-get install bsdiff
CMD ["app"]
