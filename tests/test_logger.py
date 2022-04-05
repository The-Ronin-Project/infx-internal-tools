from datetime import datetime
import json
import pytest
import unittest
import structlog

#from base import BaseTestCase
from app.app import create_app
from app.helpers.structlog import DatadogJsonFormatter
from structlog.testing import capture_logs
from structlog.testing import LogCapture


app = create_app()

def test_404(log):
    response = app.test_client().get("/404")
    assert {'event': 'The requested URL was not found on the server. If you entered the URL manually please check your spelling and try again.', 'level': 'critical', 'stack_info': True} in log.events
    

@pytest.fixture(name="log_output")
def fixture_log_output():
    return LogCapture()

@pytest.fixture(autouse=True)
def fixture_configure_structlog(log_output):
    structlog.configure(
        processors=[log_output]
    )

def test_my_stuff(log_output):
    response = app.test_client().get("/404")
    assert log_output.entries == [{'stack_info': True, 'event': 'The requested URL was not found on the server. If you entered the URL manually please check your spelling and try again.', 'log_level': 'critical'}]

def test_custom_json_formatter_add_fields():
    formatter = DatadogJsonFormatter()
    now = datetime.utcnow()
    event_payload = "Event PAYLOAD"
    record = type(
        'Record',
        (object,),
        {
            "levelname": "INFO",
            "created": now,
            "event": event_payload,
            "getMessage": lambda: event_payload,
            "exc_info": "",
            "exc_text": "",
            "msg": event_payload,
        }
    )
    result = formatter.format(record)
    json_result = json.loads(result)
    assert json_result
    assert json_result["message"] == event_payload
    assert datetime.fromisoformat(json_result["timestamp"]) >= now
