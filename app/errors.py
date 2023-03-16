from werkzeug.exceptions import BadRequest


class BadRequestWithCode(BadRequest):
    def __init__(self, code, description, http_status_code=400):
        self.code = code
        self.description = description
        self.http_status_code = http_status_code


class TerminologyExpiredError(Exception):
    pass


class TerminologyEndNullError(Exception):
    pass


class TerminologyUnderConstructionError(Exception):
    pass


class NewCodesMissingError(Exception):
    pass


class NoCodesRemovedError(Exception):
    pass


class TerminologyIsStandardError(Exception):
    pass


class TerminologyIsFHIRError(Exception):
    pass


class CodeDisplayPairDuplicatedError(Exception):
    pass
