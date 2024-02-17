-- Table: concept_maps.relationship_codes

-- DROP TABLE IF EXISTS concept_maps.relationship_codes;

CREATE TABLE IF NOT EXISTS concept_maps.relationship_codes
(
    uuid uuid NOT NULL,
    code character varying COLLATE pg_catalog."default",
    display character varying COLLATE pg_catalog."default",
    system character varying COLLATE pg_catalog."default",
    additional_context character varying COLLATE pg_catalog."default",
    special_use boolean,
    relationship_system_uuid uuid,
    CONSTRAINT relationship_pkey PRIMARY KEY (uuid),
    CONSTRAINT code_unique UNIQUE (code)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS concept_maps.relationship_codes
    OWNER to roninadmin;

-- Trigger: cm_log

-- DROP TRIGGER IF EXISTS cm_log ON concept_maps.relationship_codes;

CREATE OR REPLACE TRIGGER cm_log
    BEFORE INSERT OR DELETE OR UPDATE
    ON concept_maps.relationship_codes
    FOR EACH ROW
    EXECUTE FUNCTION concept_maps.cm_logger_trigger();