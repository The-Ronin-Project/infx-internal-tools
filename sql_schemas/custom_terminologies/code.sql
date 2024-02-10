-- Table: custom_terminologies.code

-- DROP TABLE IF EXISTS custom_terminologies.code;

CREATE TABLE IF NOT EXISTS custom_terminologies.code
(
    uuid uuid NOT NULL DEFAULT uuid_generate_v4(),
    code character varying COLLATE pg_catalog."default" NOT NULL,
    display character varying COLLATE pg_catalog."default" NOT NULL,
    terminology_version_uuid uuid NOT NULL,
    additional_data jsonb,
    depends_on_property character varying COLLATE pg_catalog."default" NOT NULL DEFAULT ''::character varying,
    depends_on_system character varying COLLATE pg_catalog."default" NOT NULL DEFAULT ''::character varying,
    depends_on_value character varying COLLATE pg_catalog."default" NOT NULL DEFAULT ''::character varying,
    depends_on_display character varying COLLATE pg_catalog."default" NOT NULL DEFAULT ''::character varying,
    created_date timestamp with time zone DEFAULT now(),
    deduplication_hash character varying COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT code_pkey PRIMARY KEY (uuid),
    CONSTRAINT code_display_depends_version UNIQUE (code, display, terminology_version_uuid, depends_on_property, depends_on_system, depends_on_value, depends_on_display)
        INCLUDE(code, display, terminology_version_uuid, depends_on_property, depends_on_system, depends_on_value, depends_on_display),
    CONSTRAINT terminology_version FOREIGN KEY (terminology_version_uuid)
        REFERENCES public.terminology_versions (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS custom_terminologies.code
    OWNER to roninadmin;
-- Index: ct_terminology_version_uuid

-- DROP INDEX IF EXISTS custom_terminologies.ct_terminology_version_uuid;

CREATE INDEX IF NOT EXISTS ct_terminology_version_uuid
    ON custom_terminologies.code USING btree
    (terminology_version_uuid ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: ct_code_id

-- DROP INDEX IF EXISTS custom_terminologies.ct_code_id;

CREATE INDEX IF NOT EXISTS ct_deduplication_hash
    ON custom_terminologies.code USING btree
    (deduplication_hash ASC NULLS LAST)
    TABLESPACE pg_default;

-- Trigger: ct_log

-- DROP TRIGGER IF EXISTS ct_log ON custom_terminologies.code;

CREATE OR REPLACE TRIGGER ct_log
    BEFORE INSERT OR DELETE OR UPDATE
    ON custom_terminologies.code
    FOR EACH ROW
    EXECUTE FUNCTION custom_terminologies.ct_logger_trigger();