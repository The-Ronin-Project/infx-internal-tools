import pandas as pd
from sqlalchemy import text
from app.database import get_db
from flask import current_app

def parse_array_in_sqlite(input):
    if current_app.config['MOCK_DB'] == True:
        return input.replace('{','').replace('}','').split(',')
    return input

class SurveyExporter:
    def __init__(self, survey_uuid, organization_uuid):
        self.survey_uuid = survey_uuid
        self.organization_uuid = organization_uuid
        self.survey_data = None
        self.symptom_uuid_to_symptom_map = None
        self.answer_uuid_to_answer_map = None
        self.next_question_uuid = {}

        self.conn = get_db()
        self.organization_name = None
        self.survey_title = None
        self.load_metadata()

    def load_metadata(self):
        # Load name of survey and name of organization
        survey_title_query = self.conn.execute(
            text(
                """
                select title
                from surveys.survey
                where uuid=:survey_uuid
                """
            ), {
                'survey_uuid': self.survey_uuid
            }
        )
        self.survey_title = survey_title_query.first().title

        organization_title_query = self.conn.execute(
            text(
                """
                select org.uuid, org.name child_name, org2.name parent_name from organizations.organizations org
                left join organizations.hierarchy hier
                on hier.target_organization_uuid=org.uuid
                left join organizations.organizations org2
                on hier.source_organization_uuid=org2.uuid
                where org.uuid=:org_uuid
                """
            ), {
                'org_uuid': self.organization_uuid
            }
        ).first()
        self.organization_name = organization_title_query.child_name + " - " + organization_title_query.parent_name

    def load_symptoms(self):
        """Load data from symptom table"""
        symptom_query = self.conn.execute("select * from surveys.symptom")
        symptoms = [x for x in symptom_query]

        self.symptom_uuid_to_symptom_map = {
            x.symptom_uuid: {
                "symptom_uuid": x.symptom_uuid,
                "provider_label": x.provider_label,
                "patient_label": x.patient_label
            } for x in symptoms
        }

    def load_answer_uuid_to_answer_map(self):
        """Load data from specific_answer table"""
        specific_answer_query = self.conn.execute(
        text("""
        select sp_an.specific_answer_uuid, specific_answer_label, generic_answer_display, 
            clinical_severity_order, next_question_slug, expected, sp_an.alert_tier default_alert_tier, last_modified_date, organization_uuid,
            al_tr.alert_tier override_alert_tier, bl.next_question_group, bl.specific_next_question
        from surveys.specific_answer sp_an
        left join surveys.alert_tiers al_tr
        on sp_an.specific_answer_uuid=al_tr.specific_answer_uuid
        and survey_uuid=:survey_uuid
        and organization_uuid=:organization_uuid
        left join surveys.branching_logic bl
        on sp_an.specific_answer_uuid=bl.answer_uuid
        and bl.question_group_uuid in 
        (select question_group_uuid
        from surveys.survey_question_group_link
        where survey_uuid=:survey_uuid)
        """), {
            'survey_uuid': self.survey_uuid,
            'organization_uuid': self.organization_uuid
        })
        self.specific_answers = [x for x in specific_answer_query]

        self.answer_uuid_to_answer_map = {
            x.specific_answer_uuid: {
                    "uuid": x.specific_answer_uuid,
                    "generic_answer_display": x.generic_answer_display.replace(' (en)', ''),
                    "clinical_severity_order": x.clinical_severity_order,
                    "next_question_slug": x.next_question_slug,
                    "alert_tier": x.override_alert_tier if x.override_alert_tier is not None else x.default_alert_tier,
                    "expected": x.expected,
                    "next_question": "||nextsymptom" if x.next_question_group is True else x.specific_next_question,
                } for x in self.specific_answers
            }

    def load_answers_for_questions(self, answer_uuid_array, present_most_severe_first=False):
        present_most_severe_first = present_most_severe_first if not None else False
        answer_array = [self.answer_uuid_to_answer_map.get(x) for x in answer_uuid_array]
        #   print(answer_array)
        #   for x in answer_array: print(x.get('clinical_severity_order')) 
        sorted_array = sorted(answer_array, key=lambda k: k['clinical_severity_order'])
        if present_most_severe_first is False:
            return sorted_array
        else:
            return reversed(sorted_array)

    def load_labels(self, answer_uuid_array, present_most_severe_first=False):
        sorted_array = self.load_answers_for_questions(answer_uuid_array, present_most_severe_first)
        return '; '.join([x.get('generic_answer_display') for x in sorted_array])

    def load_next_question_slugs(self, answer_uuid_array, next_question_uuid, present_most_severe_first=False):
        """ Return the slug of the next question """
        sorted_array = self.load_answers_for_questions(answer_uuid_array, present_most_severe_first)
        next_question_slugs = [x.get("next_question") for x in sorted_array]
        next_question_slugs = [x if x is not None else next_question_uuid for x in next_question_slugs]
        next_question_slugs = [str(x) for x in next_question_slugs]
        #   print(next_question_slugs)
        #   next_question_slugs = [x if x != 'next_symptom' else "||nextsymptom" for x in next_question_slugs]
        return "; ".join(next_question_slugs)

    def load_symptom_result(self, answer_uuid_array, present_most_severe_first=False):
        """ Return the alert tier of the answers as words """
        sorted_array = self.load_answers_for_questions(answer_uuid_array, present_most_severe_first)
        return '; '.join([x.get('alert_tier') for x in sorted_array])

    def load_symptom_result_tier(self, answer_uuid_array, present_most_severe_first=False):
        """ Return the alert tier of the answers as numbers """
        sorted_array = self.load_answers_for_questions(answer_uuid_array, present_most_severe_first)
        alert_tiers = [x.get('alert_tier') for x in sorted_array]
        alert_tier_map = {
            'Low': 1,
            'Intermediate': 2,
            'High': 3,
            'Extreme': 4
        }
        alert_tier_numbers = [str(alert_tier_map.get(x)) for x in alert_tiers]
        return '; '.join(alert_tier_numbers)

    def load_expected(self, answer_uuid_array, present_most_severe_first=False):
        """ Return the expected value of each answer (boolean) """
        sorted_array = self.load_answers_for_questions(answer_uuid_array, present_most_severe_first)
        return '; '.join(["Expected" if x.get('expected') is True else "Not Expected" for x in sorted_array])

    def generate_values(self, answer_uuid_array, present_most_severe_first=False):
        sorted_array = self.load_answers_for_questions(answer_uuid_array, present_most_severe_first)
        array_length = len(list(sorted_array))
        values_array = [str(x) for x in range(array_length)]
        if present_most_severe_first is False:
            return '; '.join(values_array)
        else:
            return '; '.join(reversed(values_array))
  

    def export_survey(self):
        """Set up our DataFrame using inital data from the survey and question tables"""
        self.load_symptoms()
        self.load_answer_uuid_to_answer_map()

        survey_query = self.conn.execute(text(
        """
        select sqgl.position as qg_pos, qgm.position as qgm_pos, * from surveys.survey_question_group_link sqgl
        join surveys.question_group_members qgm
        on sqgl.question_group_uuid = qgm.question_group_uuid
        join surveys.question ques
        on ques.question_uuid = qgm.question_uuid
        where survey_uuid=:survey_uuid
        order by sqgl.position asc, qgm.position asc
        """
        ), {"survey_uuid": self.survey_uuid})

        self.survey_data = [x for x in survey_query]

        for index, value in enumerate(self.survey_data):
            if index <= len(self.survey_data) - 2:
                self.next_question_uuid[value.question_uuid] = self.survey_data[index + 1].question_uuid
            else:
                self.next_question_uuid[value.question_uuid] = "||nextsymptom"

        survey_table = [
            self.generate_row(x, index) for index, x in enumerate(self.survey_data)
            ]

        survey_df = pd.DataFrame(survey_table)

        asd=survey_df.replace(None, '',regex='')
        for col in asd.columns:
            if asd[col].dtype=='O':
                asd[col] = asd[col].astype(str)

        return asd
    

    def get_first_question_for_each_symptom_after_symptom_select(self):
        after_symptom_select = False
        first_question_per_symptom = []
        last_qg_pos = None

        for x in self.survey_data:
            if str(x.question_uuid) == '7fc52db1-9ae8-4535-9463-c75ebc7398ca':
                after_symptom_select = True
                continue

            if after_symptom_select is False:
                continue

            if x.qg_pos != last_qg_pos:
                first_question_per_symptom.append(x)

            last_qg_pos = x.qg_pos

        return first_question_per_symptom

    def get_list_of_symptoms_in_survey(self):
        first_questions = self.get_first_question_for_each_symptom_after_symptom_select()
        labels = [self.symptom_uuid_to_symptom_map.get(parse_array_in_sqlite(x.symptom_uuids)[0]).get("patient_label") if x.symptom_uuids else "No label" for x in first_questions]
        return [x.replace('Nausea', 'Nausea/Vomiting') for x in labels]

    def get_slugs_for_symptom_start(self):
        first_questions = self.get_first_question_for_each_symptom_after_symptom_select()
        return [str(x.question_uuid) for x in first_questions]

    def generate_row(self, x, index):

        if str(x.question_uuid) == '7fc52db1-9ae8-4535-9463-c75ebc7398ca': # Select Symptoms Question
            return {
            "slug": x.question_uuid,
            "category": "Symptoms",
            "question_header": "Select Symptoms",
            "position": index + 1,
            "title": x.question_text,
            "labels": '; '.join(self.get_list_of_symptoms_in_survey() + ['None of the above']),
            "values": '; '.join(['1' for x in range(len(self.get_first_question_for_each_symptom_after_symptom_select()))] + ['0']),
            "next_question_slug": '; '.join(self.get_slugs_for_symptom_start()),
            "kind": "Multiple Choice",
            "symptom_result_clinicians_tier": '; '.join(["Low" for x in self.get_first_question_for_each_symptom_after_symptom_select()] + ["No Symptom"]),
            "symptom_result_tier": '; '.join(["1" for x in self.get_first_question_for_each_symptom_after_symptom_select()] + ["No Symptom"]),
            "symptom_result": '; '.join(["Expected" for x in self.get_first_question_for_each_symptom_after_symptom_select()] + ["No Symptom"]),
            "not_expected_reason": x.not_expected_reason,
            "provider_sees": x.provider_sees,
            "track_as": "symptom",
            "details": '',
            "topbraid_uri": x.question_uuid,
            "symptom_selection": 1,
            "survey_name": None
            }
        
        return {
            "slug": x.question_uuid,
            "category": self.symptom_uuid_to_symptom_map.get(parse_array_in_sqlite(x.symptom_uuids)[0]).get("provider_label") if x.symptom_uuids else None,
            "question_header": self.symptom_uuid_to_symptom_map.get(parse_array_in_sqlite(x.symptom_uuids)[0]).get("patient_label") if x.symptom_uuids else None,
            "position": index + 1,
            "title": x.question_text,
            "labels": self.load_labels(parse_array_in_sqlite(x.specific_answer_uuids), x.present_most_severe_first) if x.specific_answer_uuids else None,
            "values": self.generate_values(parse_array_in_sqlite(x.specific_answer_uuids), x.present_most_severe_first) if x.specific_answer_uuids else None,
            "next_question_slug": self.load_next_question_slugs(parse_array_in_sqlite(x.specific_answer_uuids), self.next_question_uuid.get(x.question_uuid), x.present_most_severe_first) if x.specific_answer_uuids else None,
            "kind": "Multiple Choice" if x.question_uuid == '7fc52db1-9ae8-4535-9463-c75ebc7398ca' else "Single Choice",
            "symptom_result_clinicians_tier": self.load_symptom_result(parse_array_in_sqlite(x.specific_answer_uuids), x.present_most_severe_first) if x.specific_answer_uuids else None,
            "symptom_result_tier": self.load_symptom_result_tier(parse_array_in_sqlite(x.specific_answer_uuids), x.present_most_severe_first) if x.specific_answer_uuids else None,
            "symptom_result": self.load_expected(parse_array_in_sqlite(x.specific_answer_uuids), x.present_most_severe_first) if x.specific_answer_uuids else None,
            "not_expected_reason": x.not_expected_reason,
            "provider_sees": x.provider_sees,
            "track_as": "symptom",
            "details": '',
            "topbraid_uri": x.question_uuid,
            "symptom_selection": 1 if x.question_uuid == '7fc52db1-9ae8-4535-9463-c75ebc7398ca' else 0,
            "survey_name": None
        }