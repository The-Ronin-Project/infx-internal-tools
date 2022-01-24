from io import StringIO
import re
from flask import Flask, jsonify, request, Response
from app.models.value_sets import *
from app.models.surveys import *

app = Flask(__name__)
app.config['MOCK_DB'] = False

@app.route('/ping')
def ping():
    return 'OK'

# FHIR endpoint
@app.route('/ValueSet/<string:uuid>/$expand')
def expand_value_set(uuid):
    force_new = request.values.get('force_new') == 'true'
    vs_version = ValueSetVersion.load(uuid)
    vs_version.expand(force_new=force_new)
    return jsonify(vs_version.serialize())

@app.route('/ValueSets/')
def get_all_value_sets_metadata():
    active_only = False if request.values.get('active_only') == 'false' else True
    return jsonify(ValueSet.load_all_value_set_metadata(active_only))

@app.route('/ValueSets/<string:name>/versions/')
def get_value_set_versions(name):
    return jsonify(ValueSet.load_version_metadata(name))

@app.route('/ValueSets/<string:name>/most_recent_active_version')
def get_most_recent_version(name):
    version = ValueSet.load_most_recent_active_version(name)
    version.expand()
    return jsonify(version.serialize())

@app.route('/ValueSets/expansions/<string:expansion_uuid>/report')
def load_expansion_report(expansion_uuid):
    report = ValueSetVersion.load_expansion_report(expansion_uuid)
    file_buffer = StringIO()
    file_buffer.write(report)
    file_buffer.seek(0)
    response = Response(
        file_buffer,
        mimetype="text/plain",
        headers={
            "Content-Disposition": f"attachment; filename={expansion_uuid}-report.csv"
        }
    )
    return response

@app.route('/surveys/<string:survey_uuid>')
def export_survey(survey_uuid):
    organization_uuid = request.values.get("organization_uuid")
    # print(survey_uuid, organization_uuid)
    exporter = SurveyExporter(survey_uuid, organization_uuid)

    file_buffer = StringIO()
    exporter.export_survey().to_csv(file_buffer)
    file_buffer.seek(0)
    response = Response(
        file_buffer, 
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={exporter.survey_title} {exporter.organization_name}.csv"
        })
    return response

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")
