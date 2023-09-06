FROM docker-proxy.devops.projectronin.io/ronin/python-builder:latest as builder

ARG USER_NAME=ronin

COPY --chown=${USER_NAME}:${USER_NAME} Pipfile ./
COPY --chown=${USER_NAME}:${USER_NAME} Pipfile.lock ./

USER root
RUN apt-get update \
  # dependencies for building Python packages
  && apt-get install -y build-essential \
  # psycopg2 dependencies
  && apt-get install -y libpq-dev \
  # Additional dependencies
  && apt-get install -y telnet netcat \
  # cleaning up unused files
  && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
  && rm -rf /var/lib/apt/lists/*

USER ${USER_NAME}:${USER_NAME}
RUN pipenv install --system --deploy

FROM docker-proxy.devops.projectronin.io/ronin/base/python-base:latest as runtime

ARG USER_NAME=ronin

COPY --from=builder --chown=${USER_NAME}:${USER_NAME} /app/.local/ /app/.local
COPY --chown=${USER_NAME}:${USER_NAME} app ./app

EXPOSE 8000
USER ${USER_NAME}
RUN mkdir ./.oci

RUN pip install \
    cryptography \
    ddtrace \
    lxml \
    numpy \
    pandas \
    psycopg2-binary

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1


# ENTRYPOINT [ "./entrypoint.sh" ]
CMD [ "python", "-m", "app" ]