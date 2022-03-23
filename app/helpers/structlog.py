import logging

import structlog


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
        structlog.processors.JSONRenderer(),
    ],
)

# First we add a StreamHandler to redirect logs to stream (by default: stdout),
# then we set the formatter to the `common_formatter` referenced above.
# Attaching this handler to a logger formats the logs as described above.
# Adding this handler to the root logger formats _all loggers in the current context_
common_handler = logging.StreamHandler()
common_handler.setFormatter(common_formatter)
