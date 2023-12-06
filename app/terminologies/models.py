import datetime
import json
import uuid
from typing import List, Dict, Union, Optional

from sqlalchemy import text
from cachetools.func import ttl_cache
import app.models.codes
from app.database import get_db
from app.errors import BadRequestWithCode, NotFoundException


@ttl_cache()
def terminology_version_uuid_lookup(fhir_uri: str, version: str):
    """
    Given a FHIR URI and version, this function retrieves the UUID of the corresponding terminology version from the database.

    Args:
        fhir_uri (str): The FHIR URI of the terminology.
        version (str): The version of the terminology.

    Returns:
        UUID: The UUID of the corresponding terminology version.
        If no result, it returns None. Caller method handles this possibility.
    """
    conn = get_db()
    result = conn.execute(
        text(
            """
            select uuid from terminology_versions
            where fhir_uri=:fhir_uri
            and version=:version
            """
        ),
        {"fhir_uri": fhir_uri, "version": version},
    ).first()
    if result:
        return result.uuid


def load_terminology_version_with_cache(terminology_version_uuid):
    """
    Eventually, this should be removed and all references replaced with the classmethod
    """
    return Terminology.load_from_cache(terminology_version_uuid)


class Terminology:
    def __init__(
        self,
        uuid,
        terminology,
        version,
        effective_start,
        effective_end,
        fhir_uri,
        fhir_terminology,
        is_standard,
    ):
        self.uuid = uuid
        self.terminology = terminology
        self.version = version
        self.effective_start = effective_start
        self.effective_end = effective_end
        self.fhir_uri = fhir_uri
        self.fhir_terminology = fhir_terminology
        self.codes = []
        self.is_standard = is_standard

    @property
    def name(self):
        return self.terminology

    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other):
        if isinstance(other, Terminology):
            if other.uuid == self.uuid:
                return True
        return False

    def __repr__(self):
        return f"Terminology(uuid={self.uuid}, name={self.terminology}, version={self.version})"

    @classmethod
    def load(cls, terminology_version_uuid):
        """
        A class method that loads a Terminology given its UUID.

        Args:
            terminology_version_uuid (UUID): The UUID of the terminology version to load.

        Returns:
            Terminology: An instance of the Terminology class with the loaded metadata.
        """

        conn = get_db()
        term_data = conn.execute(
            text(
                """
                select * from terminology_versions
                where uuid=:terminology_version_uuid
                """
            ),
            {"terminology_version_uuid": terminology_version_uuid},
        ).first()

        # When there is no data, do not create an object
        if term_data is None:
            raise NotFoundException(
                f"No data found for terminology version UUID: {terminology_version_uuid}"
            )

        # Create and return a Terminology object
        return cls(
            uuid=term_data.uuid,
            terminology=term_data.terminology,
            version=term_data.version,
            effective_start=term_data.effective_start,
            effective_end=term_data.effective_end,
            fhir_uri=term_data.fhir_uri,
            fhir_terminology=term_data.fhir_terminology,
            is_standard=term_data.is_standard,
        )

    @classmethod
    @ttl_cache()
    def load_from_cache(cls, terminology_version_uuid):
        return cls.load(terminology_version_uuid)

    @classmethod
    def load_by_fhir_uri_and_version(cls, fhir_uri: str, version: str):
        terminology_version_uuid = terminology_version_uuid_lookup(fhir_uri, version)
        if terminology_version_uuid is not None:
            return cls.load(terminology_version_uuid)
        if terminology_version_uuid is None:
            raise NotFoundException(
                f"No terminology is found with the provided fhir_uri: {fhir_uri} and version: {version}"
            )

    @classmethod
    @ttl_cache()
    def load_by_fhir_uri_and_version_from_cache(cls, fhir_uri: str, version: str):
        return cls.load_by_fhir_uri_and_version(fhir_uri, version)

    def load_content(self):
        """
        Loads the content of a FHIR terminology into the Terminology instance.

        Raises:
            NotImplementedError: If the Terminology instance is not a FHIR terminology.
        """

        if self.fhir_terminology is True:
            conn = get_db()
            content_data = conn.execute(
                text(
                    """
                    select * from fhir_defined_terminologies.code_systems_new
                    where terminology_version_uuid =:terminology_version_uuid
                    """
                ),
                {"terminology_version_uuid": self.uuid},
            )
        else:
            raise NotImplementedError(
                "Loading content for non-FHIR terminologies is not supported."
            )

        for item in content_data:
            self.codes.append(
                app.models.codes.Code(
                    system=self.fhir_uri,
                    version=self.version,
                    code=item.code,
                    display=item.display,
                    uuid=item.uuid,
                    system_name=self.terminology,
                    terminology_version=self,
                )
            )

    @classmethod
    def load_terminologies_for_value_set_version(cls, vs_version_uuid):
        """
        A class method that loads all the terminologies associated with a value set version.

        Args:
            vs_version_uuid (UUID): The UUID of the value set version.

        Returns:
            dict: A dictionary containing Terminology instances, keyed by their UUIDs.
        """
        conn = get_db()
        term_data = conn.execute(
            text(
                """
            select * 
            from terminology_versions
            where uuid in 
            (select terminology_version
            from value_sets.value_set_rule
            where value_set_version=:vs_version)
            """
            ),
            {"vs_version": vs_version_uuid},
        )
        terminologies = {
            x.uuid: Terminology(
                x.uuid,
                x.terminology,
                x.version,
                x.effective_start,
                x.effective_end,
                x.fhir_uri,
                x.fhir_terminology,
                x.is_standard
            )
            for x in term_data
        }

        return terminologies

    @classmethod
    def create_new_terminology(
        cls,
        terminology,
        version,
        effective_start,
        effective_end,
        fhir_uri,
        is_standard,
        fhir_terminology,
    ):
        """
        A class method that creates a new terminology in the database with the provided metadata.

        Args:
            terminology (str): The name of the new terminology.
            version (str): The version of the new terminology.
            effective_start (datetime.datetime): The effective start date of the new terminology.
            effective_end (datetime.datetime): The effective end date of the new terminology.
            fhir_uri (str): The FHIR URI of the new terminology.
            is_standard (bool): Whether the new terminology is standard or not.
            fhir_terminology (bool): Whether the new terminology is a FHIR terminology.

        Returns:
            Terminology: An instance of the Terminology class with the metadata of the created terminology.
        """

        conn = get_db()
        new_terminology_uuid = uuid.uuid4()
        try:
            conn.execute(
                text(
                    """
                    Insert into public.terminology_versions(uuid, terminology, version, effective_start, effective_end, fhir_uri, is_standard, fhir_terminology)
                    Values (:uuid, :terminology, :version, :effective_start, :effective_end, :fhir_uri, :is_standard, :fhir_terminology)
                    """
                ),
                {
                    "uuid": new_terminology_uuid,
                    "terminology": terminology,
                    "version": version,
                    "effective_start": effective_start,
                    "effective_end": effective_end,
                    "fhir_uri": fhir_uri,
                    "is_standard": is_standard,
                    "fhir_terminology": fhir_terminology,
                },
            )
        except Exception as e:
            conn.rollback()
            raise e
        new_terminology = conn.execute(
            text(
                """
                select * from public.terminology_versions
                where uuid=:uuid
                """
            ),
            {"uuid": new_terminology_uuid},
        ).first()
        return new_terminology

    def serialize(self):
        """
        Serializes the Terminology instance into a dictionary.

        Returns:
            dict: A dictionary representation of the Terminology instance.
        """

        return {
            "uuid": self.uuid,
            "name": self.terminology,
            "version": self.version,
            "effective_start": self.effective_start,
            "effective_end": self.effective_end,
            "fhir_uri": self.fhir_uri,
            "is_standard": self.is_standard,
            "fhir_terminology": self.fhir_terminology,
        }

    def load_latest_version(self) -> "Terminology":
        """
        Returns the latest version of the terminology
        """
        # First check if this is the most recent version
        conn = get_db()
        terminology_versions_info = conn.execute(
            text(
                """
                select * from public.terminology_versions
                where terminology=:terminology_name
                order by version desc
                """
            ),
            {"terminology_name": self.terminology},
        ).first()

        most_recent_version_uuid = terminology_versions_info.uuid
        if most_recent_version_uuid == self.uuid:
            most_recent_terminology = self
        else:
            most_recent_terminology = Terminology.load(most_recent_version_uuid)
        return most_recent_terminology

    @ttl_cache()
    def version_to_load_new_content_to(self) -> "Terminology":
        """
        A custom terminology should only have content loaded to it if its effective_end date has
        not yet passed. This method will return a Terminology instance representing the most recent version
        suitable for loading new content to, or it will create a new version and return it.
        """
        most_recent_terminology = self.load_latest_version()

        # Then check if the effective date has not yet passed
        if most_recent_terminology.effective_end is None or (
            datetime.date.today() <= most_recent_terminology.effective_end
        ):
            return most_recent_terminology

        # We need to create a new version
        current_version = most_recent_terminology.version
        try:
            if current_version.isdigit():
                new_version_string = str(int(current_version) + 1)
            else:
                new_version_string = str(int(float(current_version) + 1))
        except TypeError:
            raise Exception(
                f"Could not automatically increment version number {current_version}"
            )

        new_version = Terminology.new_terminology_version_from_previous(
            previous_version_uuid=most_recent_terminology.uuid,
            version=new_version_string,
            effective_end=None,
            effective_start=None,
        )
        return new_version

    @classmethod
    def new_terminology_version_from_previous(
        cls,
        previous_version_uuid,
        version,
        effective_start: Optional[datetime.datetime] = None,
        effective_end: Optional[datetime.datetime] = None,
    ):
        """
        A class method that creates a new terminology version based on a previous version and the provided metadata.

        This will automatically copy the contents of the previous terminology version into the new version.

        Args:
            previous_version_uuid (UUID): The UUID of the previous terminology version.
            version (str): The version of the new terminology.
            effective_start (datetime.datetime): The effective start date of the new terminology.
            effective_end (datetime.datetime): The effective end date of the new terminology.

        Returns:
            Terminology: An instance of the Terminology class with the metadata of the new terminology version.
        """

        if effective_start is None:
            effective_start = datetime.datetime.now()
        if effective_end is None:
            effective_end = datetime.datetime.now() + datetime.timedelta(days=7)
        conn = get_db()
        previous_version_metadata = conn.execute(
            text(
                """
                select terminology, fhir_uri, is_standard, fhir_terminology from public.terminology_versions
                where uuid = :previous_version_uuid 
                """
            ),
            {"previous_version_uuid": previous_version_uuid},
        ).first()
        if previous_version_metadata is None:
            raise NotFoundException(
                f"No Terminology found with UUID: {previous_version_uuid}"
            )
        version_uuid = uuid.uuid4()
        terminology = previous_version_metadata.terminology
        fhir_uri = previous_version_metadata.fhir_uri
        is_standard = previous_version_metadata.is_standard
        fhir_terminology = previous_version_metadata.fhir_terminology
        try:
            conn.execute(
                text(
                    """
                    Insert into public.terminology_versions(uuid, terminology, version, fhir_uri, is_standard, fhir_terminology, effective_start, effective_end )
                    Values (:uuid, :terminology, :version, :fhir_uri, :is_standard, :fhir_terminology, :effective_start, :effective_end)
                    """
                ),
                {
                    "uuid": version_uuid,
                    "terminology": terminology,
                    "version": version,
                    "fhir_uri": fhir_uri,
                    "is_standard": is_standard,
                    "fhir_terminology": fhir_terminology,
                    "effective_start": effective_start,
                    "effective_end": effective_end,
                },
            )
        except Exception as e:
            conn.rollback()
            raise e
        try:
            conn.execute(
                text(
                    """
                    Insert into custom_terminologies.code(code, display, terminology_version_uuid, additional_data, depends_on_value, depends_on_display, depends_on_property, depends_on_system, created_date)
                    select code, display, :version_uuid, additional_data, depends_on_value, depends_on_display, depends_on_property, depends_on_system, created_date
                    from custom_terminologies.code
                    where terminology_version_uuid = :previous_version_uuid
                    """
                ),
                {
                    "previous_version_uuid": previous_version_uuid,
                    "version_uuid": version_uuid,
                },
            )
        except Exception as e:
            conn.rollback()
            raise e
        new_term_version = conn.execute(
            text(
                """
                select * from public.terminology_versions
                where uuid = :version_uuid
                """
            ),
            {"version_uuid": version_uuid},
        ).first()
        return new_term_version

    def able_to_load_new_codes(self):
        if self.is_standard or self.fhir_terminology:
            return False, "Cannot add new codes to a standard or FHIR terminology."

        if (
            self.effective_end is not None
            and datetime.date.today() > self.effective_end
        ):
            return (
                False,
                "Cannot add new codes to a terminology that has ended its effective period.",
            )
        return True, None

    def load_new_codes_to_terminology(
        self, codes: List["app.models.codes.Code"], on_conflict_do_nothing=False
    ):
        """
        This method loads new codes into the terminology.

        Args:
        codes (List[Code]): A list of codes that should be added to the terminology.

        Raises:
        BadRequestWithCode: This exception is raised if the terminology is not a custom terminology,
        or if the terminology's effective period has ended.
        """
        # Get database connection
        conn = get_db()

        able_to_load, fail_reason = self.able_to_load_new_codes()
        if not able_to_load:
            raise BadRequestWithCode(
                code="Terminology.load_new_codes.closed_to_new_codes",
                description=fail_reason,
            )

        terminology_uuids = [code.terminology_version_uuid for code in codes]
        terminology_uuids = list(set(terminology_uuids))

        if len(terminology_uuids) > 1:
            raise BadRequestWithCode(
                "Terminology.load_new_codes.multiple_terminologies",
                "Cannot load codes to multiple terminologies at the same time",
            )
        if terminology_uuids is None or terminology_uuids[0] is None:
            raise BadRequestWithCode(
                "Terminology.load_new_codes.no_terminology",
                "Cannot load codes when no terminology is input",
            )
        if str(terminology_uuids[0]) != str(self.uuid):
            raise BadRequestWithCode(
                code="Terminology.load_new_codes.class_conflict",
                description=f"Cannot load codes to Terminology {terminology_uuids[0]} using the class for Terminology {self.uuid}",
            )

        total_count_inserted = 0
        for code in codes:
            serialized_code = code.code
            if type(serialized_code) == dict:
                serialized_code = json.dumps(serialized_code)

            # Insert new code into the database
            query_text = """
                    INSERT INTO custom_terminologies.code (uuid, code, display, additional_data, terminology_version_uuid, depends_on_value, depends_on_display, depends_on_property, depends_on_system)
                    VALUES (:uuid, :code, :display, :additional_data, :terminology_version_uuid, :depends_on_value, :depends_on_display, :depends_on_property, :depends_on_system)
                """
            if on_conflict_do_nothing:
                query_text += """ on conflict do nothing
                """
            query_text += """ returning uuid"""
            try:
                result = conn.execute(
                    text(query_text),
                    {
                        "uuid": code.custom_terminology_code_uuid
                        if code.custom_terminology_code_uuid is not None
                        else uuid.uuid4(),
                        "code": serialized_code,
                        "display": code.display,
                        "terminology_version_uuid": code.terminology_version_uuid,
                        "depends_on_value": code.depends_on_value
                        if code.depends_on_value
                        else "",
                        "depends_on_display": code.depends_on_display
                        if code.depends_on_display
                        else "",
                        "depends_on_property": code.depends_on_property
                        if code.depends_on_property
                        else "",
                        "depends_on_system": code.depends_on_system
                        if code.depends_on_system
                        else "",
                        "additional_data": json.dumps(code.additional_data),
                    },
                ).fetchall()
            except Exception as e:
                conn.rollback()
                raise e
            count_inserted = len(result)
            total_count_inserted += count_inserted
        return total_count_inserted

    def get_recent_codes(self, comparison_date):
        conn = get_db()
        recent_codes_data = conn.execute(
            text(
                """
        SELECT * FROM
        custom_terminologies.code
        WHERE
        terminology_version_uuid = :source_terminology_uuid
        AND
        created_date > :comparison_date
        """
            ),
            {
                "source_terminology_uuid": self.uuid,
                "comparison_date": comparison_date,
            },
        ).fetchall()
        recent_codes = [
            app.models.codes.Code(
                code=item.code,
                display=item.display,
                terminology_version=self,
                system=self.fhir_uri,
                version=self.version,
            )
            for item in recent_codes_data
        ]
        return recent_codes
