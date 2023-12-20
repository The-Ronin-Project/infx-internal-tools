import datetime


def convert_string_to_datetime_or_none(input_string):
    if input_string is None:
        return None
    else:
        try:
            return datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            return datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%SZ")
