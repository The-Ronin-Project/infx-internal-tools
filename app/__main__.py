import argparse

import hupper
import waitress


from .app import create_app


parser = argparse.ArgumentParser(
    description=f'Run infx-content'
)
parser.add_argument('--reload', action='store_true', default=False)
args = parser.parse_args()  # argparse.Namespace


def main():
    if args.reload:
        reloader = hupper.start_reloader('app.__main__.main')
    app = create_app()
    waitress.serve(app=app, port=8000, threads=8, host="0.0.0.0")


if __name__ == "__main__":
    main()
