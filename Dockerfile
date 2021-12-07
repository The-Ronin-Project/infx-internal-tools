FROM tiangolo/uwsgi-nginx:python3.9 as builder

WORKDIR /app
COPY Pipfile Pipfile.lock ./
RUN pip install pipenv
RUN pipenv install --dev --system

COPY . .
RUN pytest
