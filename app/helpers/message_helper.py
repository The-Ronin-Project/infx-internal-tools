def message_exception_summary(e: Exception) -> str:
    """
    Based on the input Exception object, provide a stack trace as text.
    A full stack trace is necessary to pinpoint issues triggered by frequently used constructs like terminologies.
    @return str
    """
    return f"""{e.__class__.__name__}: {e.message if hasattr(e, "message") else ""}\n{e}\n"""
