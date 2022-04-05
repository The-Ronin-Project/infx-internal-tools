FROM docker-proxy.devops.projectronin.io/ronin/python-builder:latest as builder

COPY --chown=ronin:ronin Pipfile ./
COPY --chown=ronin:ronin Pipfile.lock ./

RUN pipenv install --system --deploy

FROM docker-proxy.devops.projectronin.io/ronin/base/python-base:latest as runtime
COPY --from=builder --chown=ronin:ronin /app/.local/ /app/.local
COPY --chown=ronin:ronin app ./app

EXPOSE 8000
USER ronin
ENTRYPOINT [ "./entrypoint.sh" ]
CMD [ "python", "-m", "project" ]
