FROM docker-proxy.devops.projectronin.io/ronin/python-builder:latest as builder

COPY --chown=ronin:ronin Pipfile ./
COPY --chown=ronin:ronin Pipfile.lock ./

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

USER ronin
RUN pipenv install --system --deploy

FROM docker-proxy.devops.projectronin.io/ronin/base/python-base:latest as runtime
RUN mkdir ./.oci
COPY --from=builder --chown=ronin:ronin /app/.local/ /app/.local
COPY --chown=ronin:ronin app ./app

EXPOSE 8000
USER ronin
# ENTRYPOINT [ "/bin/bash", "-c" ]
CMD [ "python", "-m", "project" ]

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

# USER root
# RUN apt-get update \
#  # dependencies for building Python packages
#  && apt-get install -y build-essential \
#  # psycopg2 dependencies
#  && apt-get install -y libpq-dev \
#  # Additional dependencies
#  && apt-get install -y telnet netcat \
#  # cleaning up unused files
#  && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
#  && rm -rf /var/lib/apt/lists/*

# COPY ./compose/local/flask/entrypoint /entrypoint
# RUN sed -i 's/\r$//g' /entrypoint
# RUN chmod +x /entrypoint

USER ronin
COPY --chown=ronin:ronin ./compose/local/flask/start /start
# RUN sed -i 's/\r$//g' /start
RUN chmod +x /start

COPY --chown=ronin:ronin ./compose/local/flask/celery/worker/start /start-celeryworker
# RUN sed -i 's/\r$//g' /start-celeryworker
RUN chmod +x /start-celeryworker

COPY --chown=ronin:ronin ./compose/local/flask/celery/beat/start /start-celerybeat
# RUN sed -i 's/\r$//g' /start-celerybeat
RUN chmod +x /start-celerybeat

#COPY ./compose/local/flask/celery/flower/start /start-flower
#RUN sed -i 's/\r$//g' /start-flower
#RUN chmod +x /start-flower

WORKDIR /app

#ENTRYPOINT ["/entrypoint"]

