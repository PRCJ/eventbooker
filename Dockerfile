FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/eventbooker ./cmd/eventbooker \
	&& CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/burst ./cmd/burst

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && mkdir -p /data
COPY --from=build /out/eventbooker /usr/local/bin/eventbooker
COPY --from=build /out/burst /usr/local/bin/burst
ENV ADDR=:8080 \
	DB_PATH=/data/eventbooker.db \
	ADMIN_TOKEN=eventbooker-admin \
	TOKEN_SECRET=dev-token-secret-change-me \
	GOMEMLIMIT=450MiB
EXPOSE 8080
VOLUME ["/data"]
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=8 \
	CMD ["/usr/local/bin/eventbooker", "ready"]
ENTRYPOINT ["/usr/local/bin/eventbooker"]
