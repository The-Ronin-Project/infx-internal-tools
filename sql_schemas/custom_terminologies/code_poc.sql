-- Table: custom_terminologies.code_poc

-- DROP TABLE IF EXISTS custom_terminologies.code_poc;

CREATE TABLE IF NOT EXISTS custom_terminologies.code_poc
(
    uuid uuid NOT NULL,
    display character varying COLLATE pg_catalog."default" NOT NULL,
    code_schema character varying COLLATE pg_catalog."default" NOT NULL,
    code_simple character varying COLLATE pg_catalog."default",
    code_jsonb jsonb,
    code_id character varying COLLATE pg_catalog."default" NOT NULL,
    terminology_version_uuid uuid NOT NULL,
    additional_data character varying COLLATE pg_catalog."default",
    created_date timestamp with time zone DEFAULT now(),
    old_uuid uuid,
    action character varying COLLATE pg_catalog."default",
    deduplication_hash character varying COLLATE pg_catalog."default",
    CONSTRAINT code_poc_pkey PRIMARY KEY (uuid),
    CONSTRAINT code_id UNIQUE (code_id, terminology_version_uuid)
        INCLUDE(terminology_version_uuid, code_id),
    CONSTRAINT old_uuid UNIQUE (old_uuid),
    CONSTRAINT terminology_version FOREIGN KEY (terminology_version_uuid)
        REFERENCES public.terminology_versions (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS custom_terminologies.code_poc
    OWNER to roninadmin;

COMMENT ON TABLE custom_terminologies.code_poc
    IS 'proof of concept for migrating data from RCDM ConceptMap schema v4 to v5: issue table';
-- Index: ct_poc_deduplication_hash

-- DROP INDEX IF EXISTS custom_terminologies.ct_poc_deduplication_hash;

CREATE INDEX IF NOT EXISTS ct_poc_deduplication_hash
    ON custom_terminologies.code_poc USING btree
    (deduplication_hash COLLATE pg_catalog."default" ASC NULLS LAST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: ct_poc_old_uuid

-- DROP INDEX IF EXISTS custom_terminologies.ct_poc_old_uuid;

CREATE INDEX IF NOT EXISTS ct_poc_old_uuid
    ON custom_terminologies.code_poc USING btree
    (old_uuid ASC NULLS LAST)
    INCLUDE(old_uuid)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: ct_poc_terminology_version_uuid

-- DROP INDEX IF EXISTS custom_terminologies.ct_poc_terminology_version_uuid;

CREATE INDEX IF NOT EXISTS ct_poc_terminology_version_uuid
    ON custom_terminologies.code_poc USING btree
    (terminology_version_uuid ASC NULLS LAST)
    INCLUDE(terminology_version_uuid)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;