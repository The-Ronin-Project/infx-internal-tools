-- Table: custom_terminologies.code_poc_issue

-- DROP TABLE IF EXISTS custom_terminologies.code_poc_issue;

CREATE TABLE IF NOT EXISTS custom_terminologies.code_poc_issue
(
    uuid uuid NOT NULL,
    display character varying COLLATE pg_catalog."default" NOT NULL,
    code_schema character varying COLLATE pg_catalog."default",
    code_simple character varying COLLATE pg_catalog."default",
    code_jsonb jsonb,
    code_id character varying COLLATE pg_catalog."default" NOT NULL,
    terminology_version_uuid uuid NOT NULL,
    additional_data character varying COLLATE pg_catalog."default",
    created_date timestamp with time zone DEFAULT now(),
    old_uuid uuid,
    issue_type character varying COLLATE pg_catalog."default",
    action character varying COLLATE pg_catalog."default",
    deduplication_hash character varying COLLATE pg_catalog."default",
    CONSTRAINT issue_code_poc_pkey PRIMARY KEY (uuid),
    CONSTRAINT issue_old_uuid UNIQUE (old_uuid),
    CONSTRAINT issue_terminology_version FOREIGN KEY (terminology_version_uuid)
        REFERENCES public.terminology_versions (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS custom_terminologies.code_poc_issue
    OWNER to roninadmin;
-- Index: ct_poc_issue_deduplication_hash

-- DROP INDEX IF EXISTS custom_terminologies.ct_poc_issue_deduplication_hash;

CREATE INDEX IF NOT EXISTS ct_poc_issue_deduplication_hash
    ON custom_terminologies.code_poc_issue USING btree
    (deduplication_hash COLLATE pg_catalog."default" ASC NULLS LAST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: ct_poc_issue_old_uuid

-- DROP INDEX IF EXISTS custom_terminologies.ct_poc_issue_old_uuid;

CREATE INDEX IF NOT EXISTS ct_poc_issue_old_uuid
    ON custom_terminologies.code_poc_issue USING btree
    (old_uuid ASC NULLS LAST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: ct_poc_issue_terminology_version_uuid

-- DROP INDEX IF EXISTS custom_terminologies.ct_poc_issue_terminology_version_uuid;

CREATE INDEX IF NOT EXISTS ct_poc_issue_terminology_version_uuid
    ON custom_terminologies.code_poc_issue USING btree
    (terminology_version_uuid ASC NULLS LAST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;