FROM docker-proxy.devops.projectronin.io/ronin/python-builder:latest as builder

WORKDIR /app
COPY Pipfile Pipfile.lock ./
RUN pip install pipenv
RUN pipenv install --dev --system

COPY . .
RUN pytest