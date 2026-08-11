# A zou function image, for Lambda.
#
# Lambda starts a container's entrypoint and expects it to be a loop
# over the runtime api rather than a server on a port, which is what
# `zou lambda` is. Everything else in here is the image the server
# ships, so a function and a node are the same binary and the same
# postgres started a different way.
#
#   docker build -t zou .
#   docker build -f deploy/lambda/Dockerfile -t zou-fn .
#
# The architecture has to match the function's, so build with
# --platform linux/arm64 for an arm64 function and linux/amd64 for an
# x86 one.
ARG BASE=zou:latest
FROM ${BASE}

# The server image's default command is a node on ports, and none of it
# means anything here.
CMD []

# A function is configured with variables rather than arguments, so one
# image serves whichever bucket and whichever project it is pointed at.
# The credentials come from the execution role and Lambda puts them in
# the environment, session token included.
ENV ZOU_TARGET=""
ENV ZOU_REF=""

# Nothing to say about the runtime directory: only /tmp is writable in
# a function, TMPDIR points at it, and that is where zou puts it.
ENTRYPOINT ["zou", "lambda"]
