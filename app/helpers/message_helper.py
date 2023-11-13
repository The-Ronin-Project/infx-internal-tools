def message_exception_summary(e: Exception) -> str:
    return f"""{message_exception_classname(e)}: {e.message if hasattr(e, "message") else ""}\n{e}\n"""


def message_exception_classname(e: Exception) -> str:
    return f"{e.__class__.__qualname__}"
