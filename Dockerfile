FROM python:3.9-slim

ENV port 4001
ENV id 1
ENV host 127.0.0.1

WORKDIR /yacs

COPY . /yacs

CMD python worker.py ${port} ${id} ${host}
