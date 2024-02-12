-- Table: custom_terminologies.test_v4_concept_map_duplicates_auto_resolve

-- DROP TABLE IF EXISTS custom_terminologies.test_v4_concept_map_duplicates_auto_resolve;

CREATE TABLE IF NOT EXISTS custom_terminologies.test_v4_concept_map_duplicates_auto_resolve
(
    cm_title character varying COLLATE pg_catalog."default",
    cm_uuid uuid,
    cm_version character varying COLLATE pg_catalog."default",
    cm_version_uuid uuid,
    custom_terminologies_code_uuid uuid NOT NULL,
    normalized_code_value character varying COLLATE pg_catalog."default",
    normalized_display_value character varying COLLATE pg_catalog."default",
    code character varying COLLATE pg_catalog."default",
    display character varying COLLATE pg_catalog."default",
    code_deduplication_hash character varying COLLATE pg_catalog."default",
    concept_map_concept_relationship_uuid uuid,
    relationship_code character varying COLLATE pg_catalog."default",
    target_concept_code character varying COLLATE pg_catalog."default",
    target_concept_display character varying COLLATE pg_catalog."default",
    target_concept_system character varying COLLATE pg_catalog."default",
    mapping_deduplication_hash character varying COLLATE pg_catalog."default",
    fix_action character varying COLLATE pg_catalog."default",
    CONSTRAINT pkey_test_v4_concept_map_duplicates_auto_resolve PRIMARY KEY (custom_terminologies_code_uuid)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS custom_terminologies.test_v4_concept_map_duplicates_auto_resolve
    OWNER to roninadmin;ER to roninadmin;R to roninadmin;