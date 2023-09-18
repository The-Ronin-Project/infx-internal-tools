from datetime import datetime
from io import StringIO

import pandas as pd
import uuid

from sqlalchemy import text
from app.database import get_db
from flask import current_app

from app.errors import NotFoundException
from app.helpers.oci_helper import put_data_to_oci


def parse_array_in_sqlite(input):
    if current_app.config["MOCK_DB"] == True:
        return input.replace("{", "").replace("}", "").split(",")
    return input


class SurveyExporter:
    database_schema_version = 1
    object_storage_folder_name = "Questionnaires"

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
        # Load name of the active survey and name of organization
        survey_title_query = self.conn.execute(
            text(
                """
                select title
                from surveys.survey
                where uuid=:survey_uuid
                """
            ),
            {"survey_uuid": self.survey_uuid},
        ).first()
        if survey_title_query is None:
            raise NotFoundException(f"No Survey found with UUID {self.survey_uuid}")
        self.survey_title = survey_title_query.title

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
            ),
            {"org_uuid": self.organization_uuid},
        ).first()
        if organization_title_query is None:
            raise NotFoundException(f"No Organization found with UUID {self.organization_uuid}")
        self.organization_name = (
            organization_title_query.child_name
            + " - "
            + organization_title_query.parent_name
        )

    def load_symptoms(self):
        """Load data from symptom table"""
        symptom_query = self.conn.execute(text("select * from surveys.symptom"))
        symptoms = [x for x in symptom_query]

        self.symptom_uuid_to_symptom_map = {
            x.symptom_uuid: {
                "symptom_uuid": x.symptom_uuid,
                "provider_label": x.provider_label,
                "patient_label": x.patient_label,
            }
            for x in symptoms
        }

    def load_answer_uuid_to_answer_map(self):
        """Load data from specific_answer table"""
        specific_answer_query = self.conn.execute(
            text(
                """
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
        """
            ),
            {
                "survey_uuid": self.survey_uuid,
                "organization_uuid": self.organization_uuid,
            },
        )
        self.specific_answers = [x for x in specific_answer_query]

        self.answer_uuid_to_answer_map = {
            x.specific_answer_uuid: {
                "uuid": x.specific_answer_uuid,
                "generic_answer_display": x.generic_answer_display.replace(" (en)", ""),
                "clinical_severity_order": x.clinical_severity_order,
                "next_question_slug": x.next_question_slug,
                "alert_tier": x.override_alert_tier
                if x.override_alert_tier is not None
                else x.default_alert_tier,
                "expected": x.expected,
                "next_question": "||nextsymptom"
                if x.next_question_group is True
                else x.specific_next_question,
            }
            for x in self.specific_answers
        }

    def load_answers_for_questions(
        self, answer_uuid_array, present_most_severe_first=False
    ):
        present_most_severe_first = present_most_severe_first if not None else False
        answer_array = []
        for answer in answer_uuid_array:
            row = self.answer_uuid_to_answer_map.get(answer)
            if row is not None:
                answer_array.append(row)

        sorted_array = sorted(answer_array, key=lambda k: k["clinical_severity_order"])
        if present_most_severe_first is False:
            return sorted_array
        else:
            return reversed(sorted_array)

    def load_labels(self, answer_uuid_array, present_most_severe_first=False):
        sorted_array = self.load_answers_for_questions(
            answer_uuid_array, present_most_severe_first
        )
        return "; ".join([x.get("generic_answer_display") for x in sorted_array])

    def load_next_question_slugs(
        self, answer_uuid_array, next_question_uuid, present_most_severe_first=False
    ):
        """Return the slug of the next question"""
        sorted_array = self.load_answers_for_questions(
            answer_uuid_array, present_most_severe_first
        )
        next_question_slugs = [x.get("next_question") for x in sorted_array]
        next_question_slugs = [
            x if x is not None else next_question_uuid for x in next_question_slugs
        ]
        next_question_slugs = [str(x) for x in next_question_slugs]
        #   print(next_question_slugs)
        #   next_question_slugs = [x if x != 'next_symptom' else "||nextsymptom" for x in next_question_slugs]
        return "; ".join(next_question_slugs)

    def load_next_question_text(
        self, answer_uuid_array, next_question_uuid, present_most_severe_first=False
    ):
        """Return the text of the next question"""
        question_uuid_to_text = {
            x.question_uuid: x.question_text for x in self.survey_data
        }
        question_uuid_to_text["||nextsymptom"] = "||nextsymptom"

        sorted_array = self.load_answers_for_questions(
            answer_uuid_array, present_most_severe_first
        )
        next_question_slugs = [x.get("next_question") for x in sorted_array]
        next_question_slugs = [
            x if x is not None else next_question_uuid for x in next_question_slugs
        ]
        next_question_slugs = [question_uuid_to_text[x] for x in next_question_slugs]
        next_question_slugs = [str(x) for x in next_question_slugs]
        return "; ".join(next_question_slugs)

    def load_symptom_result(self, answer_uuid_array, present_most_severe_first=False):
        """Return the alert tier of the answers as words"""
        sorted_array = self.load_answers_for_questions(
            answer_uuid_array, present_most_severe_first
        )
        return "; ".join([x.get("alert_tier") for x in sorted_array])

    def load_symptom_result_tier(
        self, answer_uuid_array, present_most_severe_first=False
    ):
        """Return the alert tier of the answers as numbers"""
        sorted_array = self.load_answers_for_questions(
            answer_uuid_array, present_most_severe_first
        )
        alert_tiers = [x.get("alert_tier") for x in sorted_array]
        alert_tier_map = {"Low": 1, "Intermediate": 2, "High": 3, "Extreme": 4}
        alert_tier_numbers = [str(alert_tier_map.get(x)) for x in alert_tiers]
        return "; ".join(alert_tier_numbers)

    def load_expected(self, answer_uuid_array, present_most_severe_first=False):
        """Return the expected value of each answer (boolean)"""
        sorted_array = self.load_answers_for_questions(
            answer_uuid_array, present_most_severe_first
        )
        return "; ".join(
            [
                "Expected" if x.get("expected") is True else "Not Expected"
                for x in sorted_array
            ]
        )

    def generate_values(self, answer_uuid_array, present_most_severe_first=False):
        sorted_array = self.load_answers_for_questions(
            answer_uuid_array, present_most_severe_first
        )
        array_length = len(list(sorted_array))
        values_array = [str(x) for x in range(array_length)]
        if present_most_severe_first is False:
            return "; ".join(values_array)
        else:
            return "; ".join(reversed(values_array))

    def export_survey(self):
        """Set up our DataFrame using inital data from the survey and question tables"""
        self.load_symptoms()
        self.load_answer_uuid_to_answer_map()

        survey_query = self.conn.execute(
            text(
                """
            select sqgl.position as qg_pos, qgm.position as qgm_pos, * from surveys.survey_question_group_link sqgl
            join surveys.question_group_members qgm
            on sqgl.question_group_uuid = qgm.question_group_uuid
            join surveys.question ques
            on ques.question_uuid = qgm.question_uuid
            where survey_uuid=:survey_uuid
            order by sqgl.position asc, qgm.position asc
            """
            ),
            {"survey_uuid": self.survey_uuid},
        )

        self.survey_data = [x for x in survey_query]

        for index, value in enumerate(self.survey_data):
            if index <= len(self.survey_data) - 2:
                self.next_question_uuid[value.question_uuid] = self.survey_data[
                    index + 1
                ].question_uuid
            else:
                self.next_question_uuid[value.question_uuid] = "||nextsymptom"

        survey_table = [
            self.generate_row(x, index) for index, x in enumerate(self.survey_data)
        ]

        survey_df = pd.DataFrame(survey_table)

        asd = survey_df.replace(None, "", regex="")
        for col in asd.columns:
            if asd[col].dtype == "O":
                asd[col] = asd[col].astype(str)

        return asd

    def get_first_question_for_each_symptom_after_symptom_select(self):
        after_symptom_select = False
        first_question_per_symptom = []
        last_qg_pos = None

        for x in self.survey_data:
            if (
                str(x.question_uuid) == "7fc52db1-9ae8-4535-9463-c75ebc7398ca"
                or str(x.question_uuid) == "54deecbb-cba0-4fd8-9e10-666f3951f4cb"
            ):
                after_symptom_select = True
                continue

            if after_symptom_select is False:
                continue

            if x.qg_pos != last_qg_pos:
                first_question_per_symptom.append(x)

            last_qg_pos = x.qg_pos

        return first_question_per_symptom

    def get_list_of_symptoms_in_survey(self):
        first_questions = (
            self.get_first_question_for_each_symptom_after_symptom_select()
        )
        labels = [
            self.symptom_uuid_to_symptom_map.get(
                parse_array_in_sqlite(x.symptom_uuids)[0]
            ).get("patient_label")
            if x.symptom_uuids
            else "No label"
            for x in first_questions
        ]
        return [x.replace("Nausea", "Nausea/Vomiting") for x in labels]

    def get_slugs_for_symptom_start(self):
        first_questions = (
            self.get_first_question_for_each_symptom_after_symptom_select()
        )
        return [str(x.question_uuid) for x in first_questions]

    def generate_row(self, x, index):

        if (
            str(x.question_uuid) == "7fc52db1-9ae8-4535-9463-c75ebc7398ca"
            or str(x.question_uuid) == "54deecbb-cba0-4fd8-9e10-666f3951f4cb"
        ):  # Select Symptoms Question
            return {
                "slug": x.question_uuid,
                "category": "Symptoms",
                "question_header": "Select Symptoms",
                "position": index + 1,
                "title": x.question_text,
                "labels": "; ".join(
                    self.get_list_of_symptoms_in_survey() + ["None of the above"]
                ),
                "values": "; ".join(
                    [
                        "1"
                        for x in range(
                            len(
                                self.get_first_question_for_each_symptom_after_symptom_select()
                            )
                        )
                    ]
                    + ["0"]
                ),
                "next_question_slug": "; ".join(self.get_slugs_for_symptom_start()),
                "next_question_text": "N/A",
                "kind": "Multiple Choice",
                "symptom_result_clinicians_tier": "; ".join(
                    [
                        "Low"
                        for x in self.get_first_question_for_each_symptom_after_symptom_select()
                    ]
                    + ["No Symptom"]
                ),
                "symptom_result_tier": "; ".join(
                    [
                        "1"
                        for x in self.get_first_question_for_each_symptom_after_symptom_select()
                    ]
                    + ["No Symptom"]
                ),
                "symptom_result": "; ".join(
                    [
                        "Expected"
                        for x in self.get_first_question_for_each_symptom_after_symptom_select()
                    ]
                    + ["No Symptom"]
                ),
                "not_expected_reason": x.not_expected_reason,
                "provider_sees": x.provider_sees,
                "track_as": "symptom",
                "details": "",
                "topbraid_uri": x.question_uuid,
                "symptom_selection": 1,
                "survey_name": None,
            }

        return {
            "slug": x.question_uuid,
            "category": self.symptom_uuid_to_symptom_map.get(
                parse_array_in_sqlite(x.symptom_uuids)[0]
            ).get("provider_label")
            if x.symptom_uuids
            else None,
            "question_header": self.symptom_uuid_to_symptom_map.get(
                parse_array_in_sqlite(x.symptom_uuids)[0]
            ).get("patient_label")
            if x.symptom_uuids
            else None,
            "position": index + 1,
            "title": x.question_text,
            "labels": self.load_labels(
                parse_array_in_sqlite(x.specific_answer_uuids),
                x.present_most_severe_first,
            )
            if x.specific_answer_uuids
            else None,
            "values": self.generate_values(
                parse_array_in_sqlite(x.specific_answer_uuids),
                x.present_most_severe_first,
            )
            if x.specific_answer_uuids
            else None,
            "next_question_slug": self.load_next_question_slugs(
                parse_array_in_sqlite(x.specific_answer_uuids),
                self.next_question_uuid.get(x.question_uuid),
                x.present_most_severe_first,
            )
            if x.specific_answer_uuids
            else None,
            "next_question_text": self.load_next_question_text(
                parse_array_in_sqlite(x.specific_answer_uuids),
                self.next_question_uuid.get(x.question_uuid),
                x.present_most_severe_first,
            )
            if x.specific_answer_uuids
            else None,
            "kind": "Multiple Choice"
            if x.question_uuid == "7fc52db1-9ae8-4535-9463-c75ebc7398ca"
            else "Single Choice",
            "symptom_result_clinicians_tier": self.load_symptom_result(
                parse_array_in_sqlite(x.specific_answer_uuids),
                x.present_most_severe_first,
            )
            if x.specific_answer_uuids
            else None,
            "symptom_result_tier": self.load_symptom_result_tier(
                parse_array_in_sqlite(x.specific_answer_uuids),
                x.present_most_severe_first,
            )
            if x.specific_answer_uuids
            else None,
            "symptom_result": self.load_expected(
                parse_array_in_sqlite(x.specific_answer_uuids),
                x.present_most_severe_first,
            )
            if x.specific_answer_uuids
            else None,
            "not_expected_reason": x.not_expected_reason,
            "provider_sees": x.provider_sees,
            "track_as": "symptom",
            "details": "",
            "topbraid_uri": x.question_uuid,
            "symptom_selection": 1
            if x.question_uuid == "7fc52db1-9ae8-4535-9463-c75ebc7398ca"
            else 0,
            "survey_name": None,
        }

    @classmethod
    def load_most_recent_active_version(cls, survey_uuid, organization_uuid):
        """
        Get the most recent version of the survey in any environment. If never published, publish version 1.
        For now, we publish to dev, stage, and prod environments all in one step, so environment is not an input.
        Note that the SurveyExporter only publishes active surveys
        @rtype: SurveyVersion
        """
        conn = get_db()
        results = conn.execute(
            text(
                """
                select s.uuid as survey_uuid, s.status as status,
                sv.uuid as uuid, sv.organization_uuid as organization_uuid, sv.version_number as version_number
                from surveys.survey_version sv join surveys.survey s on sv.survey_uuid = s.uuid 
                where sv.survey_uuid = :survey_uuid and sv.organization_uuid = :organization_uuid
                order by version desc
                limit 1
                """
            ),
            {
                "survey_uuid": survey_uuid,
                "organization_uuid": organization_uuid
            }
        )
        recent_version = results.first()
        if recent_version is None:
            raise NotFoundException(f"No Survey with UUID {survey_uuid} found.")
        if recent_version.status == 'active':
            if recent_version.uuid is None:
                return SurveyVersion.create(survey_uuid, organization_uuid, version=1)
            else:
                return SurveyVersion.load(recent_version.uuid)
        else:
            raise NotFoundException(f"No active Survey with UUID {survey_uuid} found. Status is {recent_version.status}")

    def publish_to_object_store(self, content, content_type="csv"):
        """
        Publish the most recent version of the survey to object storage in dev, stage, and prod environments.
        For now, we publish to dev, stage, and prod environments all in one step, so environment is not an input.
        @param content: content to output to file
        @param content_type: "csv" or "json"
        """
        environment_options = ["dev", "stage", "prod"]
        # @param environment: may be 'dev' 'stage' 'prod' for cumulative updates or 'dev,stage,prod' for 3-in-1 updates
        # environment_options = environment.split(",")

        # write current version CSV output to OCI

        # todo: after survey_version table exists - uncomment the next 2 lines
        # survey_version = self.load_most_recent_version(self.survey_uuid, self.organization_uuid)
        # publish_version_number = survey_version.version
        # todo: after survey_version table exists - delete the next 1 line
        publish_version_number = 1

        for env in environment_options:
            put_data_to_oci(
                content=content,
                oci_root=self.object_storage_folder_name,
                resource_schema_version=f"{self.database_schema_version}",
                release_status=env,
                resource_id=f"{self.survey_uuid}/{self.organization_uuid}",
                resource_version=publish_version_number,
                content_type=content_type,
            )

        # todo: after survey_version table exists - uncomment the next 4 lines
        # if write did not raise an exception, update current version and (if dev) create new version in database
        # survey_version.update()
        # if environment_options[0] == "dev":
        #     survey_version.create_new_version_from_specified_previous(survey_version.uuid)


class SurveyVersion:
    def __init__(
        self,
        version_uuid,
        effective_start,
        effective_end,
        version,
        survey_uuid,
        organization_uuid,
        status,
        description,
        comments,
    ):
        self.uuid = version_uuid
        self.effective_start = effective_start
        self.effective_end = effective_end
        self.version = version
        self.survey_uuid = survey_uuid
        self.organization_uuid = organization_uuid
        self.status = status
        self.description = description
        self.comments = comments

    def __repr__(self):
        return f"<SurveyVersion uuid={self.uuid}, survey_uuid={self.survey_uuid}, organization_uuid={self.organization_uuid}, version={self.version}>"

    @classmethod
    def create(
        cls,
        survey_uuid,
        organization_uuid,
        version: int
    ):
        """
        Only the essential fields are used at this time. created_date is now() and uuid is generated during create.
        Initial status is "pending".
        """
        new_version_uuid = uuid.uuid4()
        conn = get_db()
        conn.execute(
            text(
                """  
                insert into survey.survey_version  
                (uuid, survey_uuid, organization_uuid, status, created_date, version)  
                values  
                (:new_version_uuid, :survey_uuid, :organization_uuid, :status, :created_date, :version)  
                """
            ),
            {
                "new_version_uuid": new_version_uuid,
                # "effective_start": effective_start,
                # "effective_end": effective_end,
                "survey_uuid": survey_uuid,
                "organization_uuid": organization_uuid,
                "status": "pending",
                # "version_description": description,
                # "version_comments": comments,
                "created_date": datetime.now(),
                "version": version,
            },
        )
        return cls.load(new_version_uuid)

    @classmethod
    def load(cls, version_uuid):
        if version_uuid is None:
            raise NotFoundException(f"Survey Version not found")

        conn = get_db()
        version_data = conn.execute(
            text(
                """
                select * from survey.survey_version where uuid=:uuid
                """
            ),
            {"uuid": version_uuid},
        ).first()
        if version_data is None:
            raise NotFoundException(f"Unable to find Survey Version with UUID: {version_uuid}")

        survey_version = cls(
            version_data.uuid,
            version_data.effective_start,
            version_data.effective_end,
            version_data.version,
            version_data.survey_uuid,
            version_data.organization_uuid,
            version_data.status,
            version_data.description,
            version_data.comments,
        )
        return survey_version

    @classmethod
    def create_new_version_from_specified_previous(
        cls,
        version_uuid
    ):
        """
        Only the essential fields are used at this time. created_date is now() and uuid is generated during create.
        Initial status is "pending".
        """
        # Load the input survey_version
        input_version = cls.load(version_uuid)

        # Create a new survey_version
        new_version_uuid = uuid.uuid4()
        new_version = input_version.version + 1
        conn = get_db()
        conn.execute(
            text(
                """  
                INSERT INTO survey.survey_version  
                (uuid, survey_uuid, organization_uuid, status, created_date, version)  
                VALUES  
                (:new_version_uuid, :survey_uuid, :organization_uuid, :status, :created_date, :version)  
                """
            ),
            {
                "new_version_uuid": new_version_uuid,
                # "effective_start": input_version.effective_start,
                # "effective_end": input_version.effective_end,
                "survey_uuid": input_version.survey_uuid,
                "organization_uuid": input_version.organization_uuid,
                "status": "pending",
                # "description": new_version_description or input_version.description,
                # "comments": new_version_comments or input_version.comments,
                "created_date": datetime.now(),
                "version": new_version,
            },
        )

        # Return the new SurveyVersion object
        return cls.load(new_version_uuid)

    def update(self):
        """
        The only update is to set status to 'dev' 'dev,stage' or 'dev,stage,prod' when publishing the version to OCI.
        For now, we publish to dev, stage, and prod environments all in one step, so there is no environment parameter.
        """
        environment = "dev,stage,prod"
        # @param environment: may be 'dev' 'stage' 'prod' for cumulative updates or 'dev,stage,prod' for 3-in-1 updates
        if self.status == "pending":
            status_update = environment
        elif environment not in self.status:
            status_update = ",".join([self.status, environment])
        else:
            return

        conn = get_db()
        result = conn.execute(
            text(
                """  
                update survey.survey_version SET status=:status where version=:version and uuid=:uuid
                """
            ),
            {
                "status": status_update,
                "version": self.version,
                "uuid": self.uuid,
            }
        )

        return status_update
