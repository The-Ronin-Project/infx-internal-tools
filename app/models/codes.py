class Code:
    def __init__(self, system, version, code, display):
        self.system = system
        self.version = version
        self.code = code
        self.display = display

    def __repr__(self):
        return f"Code({self.code}, {self.display}, {self.system}, {self.version})"

    def __hash__(self) -> int:
        return hash(self.__repr__())

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Code):
            return (self.code == other.code) and (self.display == other.display) and (self.system == other.system) and (self.version == other.version)
        return False

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