# ONE DOCKERFILE FOR ALL FOUR BINARIES, selected by build arg.
#
# The alternative — four Dockerfiles — would be four copies of the same eight
# lines, and the failure mode of that is well known: someone fixes the Go
# version in one of them and not the other three. Here the build stage is
# shared, so the four images differ only in which package they compile.
FROM golang:1.26-alpine AS build

WORKDIR /src

# Dependencies first, so editing a .go file does not re-download the module
# graph. This is the only reason the two COPY steps are split.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG CMD
RUN test -n "$CMD" || (echo "build arg CMD is required (producer|consumer|admin|topic-init)" && exit 1)

# CGO off, so the binary does not depend on the builder's libc.
# -trimpath keeps the builder's absolute paths out of the binary: they are noise
# in a stack trace and they leak the layout of whoever built the image.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/${CMD}

# ── runtime ──────────────────────────────────────────────────────────────────
#
# alpine rather than scratch, for one reason that matters in a lab: a scratch
# image has no shell, so `docker compose exec` gives you nothing when something
# is wrong. A demo people are meant to poke at should be pokeable.
FROM alpine:3.21

RUN adduser -D -u 10001 lab
USER lab

COPY --from=build /out/service /home/lab/service

ENTRYPOINT ["/home/lab/service"]
