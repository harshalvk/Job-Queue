# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder

WORKDIR /src

# cache dependency downloads separately from source changes
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG CMD_PATH
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/app ./${CMD_PATH}

FROM gcr.io/distroless/static-debian12

COPY --from=builder /out/app /app

ENTRYPOINT [ "/app" ]
