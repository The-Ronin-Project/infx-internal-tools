import argparse
import socket
import sys

import hupper
import waitress

from celery.apps.beat import Beat
from flower.app import Flower

from .app import create_app
from .tasks import celery_app

parser = argparse.ArgumentParser(description=f"Run infx-internal-tools: {sys.argv[0]}")
parser.add_argument("--reload", action="store_true", default=False)
parser.add_argument("--worker", action="store_true", default=False)
parser.add_argument("--beat", action="store_true", default=False)
parser.add_argument("--flower", action="store_true", default=False)
parser.add_argument("--ping", action="store_true", default=False)
parser.add_argument("--log-level", default="info")
args: argparse.Namespace = parser.parse_args()


def main():
    app = create_app()
    log_level = f"--loglevel={args.log_level}"
    if args.ping:
        return celery_app.start(["inspect", "ping", "-d", f"celery@{socket.gethostname()}"])
    if args.worker:
        return celery_app.start(["worker", log_level])
    if args.beat:
        return celery_app.start(["beat", log_level])
    if args.flower:
        return celery_app.start(["flower", log_level])
    if args.reload:
        reloader = hupper.start_reloader("app.__main__.main")
    waitress.serve(app=app, port=8000, threads=8, host="0.0.0.0")


if __name__ == "__main__":
    main()
