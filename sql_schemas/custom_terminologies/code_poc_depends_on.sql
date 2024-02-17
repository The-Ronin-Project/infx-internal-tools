-- Table: custom_terminologies.code_poc_depends_on

-- DROP TABLE IF EXISTS custom_terminologies.code_poc_depends_on;

CREATE TABLE IF NOT EXISTS custom_terminologies.code_poc_depends_on
(
    uuid uuid NOT NULL,
    sequence integer NOT NULL,
    depends_on_property character varying COLLATE pg_catalog."default",
    depends_on_system character varying COLLATE pg_catalog."default",
    depends_on_display character varying COLLATE pg_catalog."default",
    depends_on_value_schema character varying COLLATE pg_catalog."default" NOT NULL,
    depends_on_value_simple character varying COLLATE pg_catalog."default",
    depends_on_value_jsonb jsonb,
    code_uuid uuid NOT NULL,
    action character varying COLLATE pg_catalog."default",
    CONSTRAINT code_poc_depends_on_pkey PRIMARY KEY (uuid),
    CONSTRAINT code_poc_depends_on_terminology_code_uuid FOREIGN KEY (code_uuid)
        REFERENCES custom_terminologies.code_poc (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS custom_terminologies.code_poc_depends_on
    OWNER to roninadmin;

COMMENT ON TABLE custom_terminologies.code_poc_depends_on
    IS 'depends_on list members for a row in the code_poc table';