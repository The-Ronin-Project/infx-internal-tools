from werkzeug.exceptions import BadRequest, InternalServerError


class BadRequestWithCode(BadRequest):
    def __init__(self, code, description, http_status_code=400):
        self.code = code
        self.description = description
        self.http_status_code = http_status_code


class NotFoundException(Exception):
    def __init__(self, message):
        self.message = message


class BadDataError(InternalServerError):
    def __init__(self, code, description, errors, http_status_code=409):
        self.code = code
        self.description = description
        self.errors = errors
        self.http_status_code = http_status_code


# Errors for ConceptMaps
class BadSourceCodeError(InternalServerError):
    def __init__(self, code, description, errors, http_status_code=409):
        self.code = code
        self.description = description
        self.errors = errors
        self.http_status_code = http_status_code


class DuplicateTargetError(InternalServerError):
    def __init__(self, code, description, errors, http_status_code=409):
        self.code = code
        self.description = description
        self.errors = errors
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


class DataIntegrityError(Exception):
    def __init__(self, message):
        self.message = message
