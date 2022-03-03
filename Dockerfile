FROM docker-proxy.devops.projectronin.io/ronin/base/python-builder:1.0.0 as builder

WORKDIR /app
COPY --chown=ronin:ronin Pipfile Pipfile.lock ./
USER ronin
RUN pipenv install --dev --system 

FROM docker-proxy.devops.projectronin.io/ronin/base/python-base:1.0.0 as runtime

USER 0
RUN apt update -y \
    && apt install nginx build-essential uwsgi-plugin-python3 -y \
    && apt clean \
    && pip install uwsgi \
    && chown -R ronin:ronin /var/log/nginx /var/lib/nginx
USER ronin
COPY --from=builder --chown=ronin:ronin /app/.local /app/.local
COPY --chown=ronin:ronin ./resources/nginx.uwsgi.conf /etc/nginx/nginx.conf
COPY --chown=ronin:ronin ./resources/uwsgi.ini /etc/uwsgi/
COPY --chown=ronin:ronin ./resources/start.sh conftest.py ./
COPY --chown=ronin:ronin app/ app/
COPY --chown=ronin:ronin tests/ tests/
CMD [ "./start.sh" ]