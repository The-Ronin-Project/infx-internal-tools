-- Table: custom_terminologies.test_code_document_duplicates_review

-- DROP TABLE IF EXISTS custom_terminologies.test_code_document_duplicates_review;

CREATE TABLE IF NOT EXISTS custom_terminologies.test_code_document_duplicates_review
(
    custom_terminologies_code_uuid uuid NOT NULL,
    normalized_code_value character varying COLLATE pg_catalog."default",
    display character varying COLLATE pg_catalog."default",
    depends_on_property character varying COLLATE pg_catalog."default",
    depends_on_value character varying COLLATE pg_catalog."default",
    cm_title character varying COLLATE pg_catalog."default",
    cm_uuid uuid,
    cm_version character varying COLLATE pg_catalog."default",
    cm_version_uuid uuid,
    code character varying COLLATE pg_catalog."default",
    target_concept_code character varying COLLATE pg_catalog."default",
    target_concept_display character varying COLLATE pg_catalog."default",
    target_concept_system character varying COLLATE pg_catalog."default",
    concept_map_concept_relationship_uuid uuid,
    fix_action character varying COLLATE pg_catalog."default",
    deduplication_hash character varying COLLATE pg_catalog."default",
    CONSTRAINT test_code_document_duplicates_review_pkey PRIMARY KEY (custom_terminologies_code_uuid)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS custom_terminologies.test_code_document_duplicates_review
    OWNER to roninadmin;