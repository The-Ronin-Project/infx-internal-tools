class Code:
    def __init__(self, system, version, code, display):
        self.system = system
        self.version = version
        self.code = code
        self.display = display

    def serialize(self, with_system_and_version=True):
        serialized = {
            "system": self.system,
            "version": self.version,
            "code": self.code,
            "display": self.display
        }

        if with_system_and_version is False:
            serialized.pop('system')
            serialized.pop('version')

        return serialized