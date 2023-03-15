import datetime
import uuid
from sqlalchemy import text
from functools import lru_cache
from app.database import get_db
import app.models.codes


@lru_cache(maxsize=None)
def terminology_version_uuid_lookup(fhir_uri, version):
    """
    Given a FHIR URI and version, this function retrieves the UUID of the corresponding terminology version from the database.

    Args:
        fhir_uri (str): The FHIR URI of the terminology.
        version (str): The version of the terminology.

    Returns:
        UUID: The UUID of the corresponding terminology version.
    """
    conn = get_db()
    result = conn.execute(
        text(
            """
            select * from public.terminology_versions
            where fhir_uri=:fhir_uri
            and version=:version
            """
        ),
        {"fhir_uri": fhir_uri, "version": version},
    ).first()
    return result.uuid


class Terminology:
    def __init__(
        self,
        uuid,
        name,
        version,
        effective_start,
        effective_end,
        fhir_uri,
        fhir_terminology,
    ):
        self.uuid = uuid
        self.name = name
        self.version = version
        self.effective_start = effective_start
        self.effective_end = effective_end
        self.fhir_uri = fhir_uri
        self.fhir_terminology = fhir_terminology
        self.codes = []
        self.is_standard = None

    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other):
        if isinstance(other, Terminology):
            if other.uuid == self.uuid:
                return True
        return False

    def __repr__(self):
        return f"Terminology({self.uuid})"

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

        return cls(
            uuid=term_data.uuid,
            name=term_data.terminology,
            version=term_data.version,
            effective_start=term_data.effective_start,
            effective_end=term_data.effective_end,
            fhir_uri=term_data.fhir_uri,
            fhir_terminology=term_data.fhir_terminology,
        )

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
                    system_name=self.name,
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

    @classmethod
    def new_terminology_version_from_previous(
        cls,
        previous_version_uuid,
        version,
        effective_start,
        effective_end,
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
        version_uuid = uuid.uuid4()
        terminology = previous_version_metadata.terminology
        fhir_uri = previous_version_metadata.fhir_uri
        is_standard = previous_version_metadata.is_standard
        fhir_terminology = previous_version_metadata.fhir_terminology
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

        conn.execute(
            text(
                """
                Insert into custom_terminologies.code(code, display, terminology_version_uuid, additional_data)
                select code, display, :version_uuid, additional_data
                from custom_terminologies.code
                where terminology_version_uuid = :previous_version_uuid
                """
            ),
            {
                "previous_version_uuid": previous_version_uuid,
                "version_uuid": version_uuid,
            },
        )
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