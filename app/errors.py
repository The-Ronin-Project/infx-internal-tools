
from flask import Flask, jsonify, request, Response, make_response
from app.models.concept_maps import *


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


@app.errorhandler(TerminologyExpiredError)
def handle_terminology_end_date_expired_error(error):
    message = {
        'error': 'Terminology version expired',
        'message': 'The effective end date for the value set has passed. A new terminology version will be created.'
    }
    return jsonify(message), 409


@app.errorhandler(TerminologyEndNullError)
def handle_terminology_end_date_null_error(error):
    message = {
        'error': 'Invalid terminology request',
        'message': 'The effective end date for the value set is null. Specify a valid end date.'
    }
    return jsonify(message), 422


@app.errorhandler(TerminologyUnderConstructionError)
def handle_terminology_under_construction_error(error):
    message = {
        'error': 'Terminology version is actively under construction',
        'message': 'The terminology is being edited and cannot undergo automapping. Publish the terminology version'
    }
    return jsonify(message), 409


@app.errorhandler(NewCodesMissingError)
def handle_new_codes_missing_error(error):
    message = {
        'error': 'New terminology codes are missing',
        'message': 'The newly added codes are missing after the terminology update. Investigate version differences'
    }
    return jsonify(message), 424


@app.errorhandler(NoCodesRemovedError)
def handle_no_codes_removed_error(error):
    message = {
        'error': 'No codes were removed',
        'message': 'No codes were removed following the addition of new codes. Investigate version differences'
    }
    return jsonify(message), 424


@app.errorhandler(TerminologyIsStandardError)
def handle_terminology_is_standard_error(error):
    message = {
        'error': 'The terminology is a standard terminology and cannot be edited.',
        'message': 'The terminology is a standard so we should not be trying to auto map it'
    }
    return jsonify(message), 409


@app.errorhandler(TerminologyIsFHIRError)
def handle_terminology_is_standard_error(error):
    message = {
        'error': 'The terminology is a FHIR and cannot be edited.',
        'message': 'The terminology is a FHIR so we should not be trying to auto map it'
    }
    return jsonify(message), 409
