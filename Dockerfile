ARG PYTHON_VERSION=3.9

FROM docker-proxy.devops.projectronin.io/python:${PYTHON_VERSION}-slim


RUN addgroup \
    --system \
    --gid 1000 \
    ronin \ 
  && adduser \
    --home /app \
    --system \
    --disabled-password \
    --uid 1000 \
    --ingroup ronin \
    ronin \
  && chown -R ronin:ronin /app

WORKDIR /app

COPY --chown=ronin:ronin Pipfile ./
COPY --chown=ronin:ronin Pipfile.lock ./
COPY --chown=ronin:ronin resources/pip.conf .config/pip/

RUN apt-get update \
  && apt-get install -y build-essential libpq-dev \
  && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
  && rm -rf /var/lib/apt/lists/* \
  && python3 -m pip install pipenv

USER ronin:ronin
RUN pipenv install --system --deploy

EXPOSE 8000
COPY --chown=ronin:ronin app ./app

RUN mkdir ./.oci

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

CMD [ "python", "-m", "app" ]