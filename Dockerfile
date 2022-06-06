ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8
ARG IMAGE_VARIANT=slim-buster

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

WORKDIR /app/

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt
