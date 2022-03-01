FROM docker-proxy.devops.projectronin.io/ronin/python-builder:latest as builder

WORKDIR /app
COPY Pipfile Pipfile.lock ./
USER ronin
RUN pip install pipenv
RUN pipenv install --dev --system
USER 0
RUN apt update -y && apt install nginx uwsgi -y && apt clean
USER ronin
COPY --chown=ronin:ronin . .
RUN pytest