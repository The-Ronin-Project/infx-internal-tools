FROM https://docker-proxy.devops.projectronin.io/v2/ronin/python-builder/manifests/latest as builder

WORKDIR /app
COPY Pipfile Pipfile.lock ./
RUN pip install pipenv
RUN pipenv install --dev --system

COPY . .
RUN pytest
