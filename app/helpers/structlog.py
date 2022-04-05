import logging
from datetime import datetime

from pythonjsonlogger import jsonlogger

import structlog


class DatadogJsonFormatter(jsonlogger.JsonFormatter):
    """
    Class that handles processing the structlog configurations below into JSON
    that DataDog can handle nicely. Motivation and most of the thought process
    credit to https://github.com/madzak/python-json-logger#customizing-fields
    """

    def add_fields(self, log_record, record, message_dict):
        super(DatadogJsonFormatter, self).add_fields(
            log_record, record, message_dict)
        # ensure here that all timestamps are coerced to ISO format
        if not log_record.get('timestamp'):
            now = datetime.utcnow().isoformat()
            log_record['timestamp'] = now
        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname
        if log_record.get('msg'):
            message = log_record.get('message', "")
            message += log_record.get('msg') or ""
            log_record['message'] = message
        if log_record.get('event'):
            # use structlog `event` as stdlib `message`
            log_record['message'] = log_record.get('event')


def config_structlog():
    """
    This method configures Structlog to prepare to send all of its messages to
    the standard logging library. Reference https://www.structlog.org/en/stable/api.html#structlog.stdlib.ProcessorFormatter

    The last entry in `processors=` MUST be the `wrap_for_formatter` method of
    the ProcessorFormatter class in order for the standard logging library to
    pick up the data.
    """
    structlog.configure_once(
        processors=[
            # These processors are _specific_ to our app. If you want to enrich
            # logs from this app only, add them here.
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter
        ],
        # This is the factory that Structlog uses when it creates loggers. This
        # ensures that we're always dealing with apples and not oranges; i.e.
        # __all loggers are ``logging.Logger``s__
        logger_factory=structlog.stdlib.LoggerFactory(),
    )


# This symbol is a standard logging library formatter that uses Structlog
# processors. Add this formatter to the base config of logging to use it.
common_formatter = structlog.stdlib.ProcessorFormatter(
    processors=[
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.ExceptionPrettyPrinter(),
    ],
)

# First we add a StreamHandler to redirect logs to stream (by default: stdout),
# then we set the formatter to the `common_formatter` referenced above.
# Attaching this handler to a logger formats the logs as described above.
# Adding this handler to the root logger formats _all loggers in the current context_
common_handler = logging.StreamHandler()
common_handler.setFormatter(
    DatadogJsonFormatter('%(timestamp)s %(level)s %(name)s %(message)s')
)
