FROM python:3.10

ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt

RUN python -m pip install -U pip &&\
              pip install -r requirements.txt &&\
              rm requirements.txt


WORKDIR /app
COPY ./app .



