from io import StringIO
from flask import Flask, jsonify, request, Response
from app.models.value_sets import *
from app.models.surveys import *
from ddtrace import patch_all
patch_all()

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

@app.route('/ValueSets/<string:name>/versions/')
def get_value_set_versions(name):
    return jsonify(ValueSet.load_version_metadata(name))

@app.route('/ValueSets/<string:name>/most_recent_active_version')
def get_most_recent_version(name):
    version = ValueSet.load_most_recent_active_version(name)
    version.expand()
    return jsonify(version.serialize())

@app.route('/surveys/<string:survey_uuid>')
def export_survey(survey_uuid):
    organization_uuid = request.values.get("organization_uuid")
    print(survey_uuid, organization_uuid)
    exporter = SurveyExporter(survey_uuid, organization_uuid)

    file_buffer = StringIO()
    exporter.export_survey().to_csv(file_buffer)
    file_buffer.seek(0)
    response = Response(
        file_buffer, 
        mimetype="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=export.csv"
        })
    return response

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")
