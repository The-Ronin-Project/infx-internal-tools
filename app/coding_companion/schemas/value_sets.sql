CREATE SCHEMA value_sets;

CREATE TABLE value_sets.expansion (
    uuid uuid NOT NULL,
    vs_version_uuid uuid NOT NULL,
    "timestamp" timestamp with time zone,
    report text
);

CREATE TABLE value_sets.expansion_member (
    expansion_uuid uuid NOT NULL,
    code character varying NOT NULL,
    display character varying NOT NULL,
    system character varying,
    version character varying,
    uuid uuid DEFAULT public.uuid_generate_v4() NOT NULL
);

CREATE TABLE value_sets.expansion_member_test (
    expansion_uuid uuid NOT NULL,
    code character varying NOT NULL,
    display character varying NOT NULL,
    system character varying,
    version character varying
);

CREATE TABLE value_sets.explicitly_included_code (
    uuid uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    vs_version_uuid uuid NOT NULL,
    code_uuid uuid NOT NULL,
    review_status character varying NOT NULL
);

CREATE TABLE value_sets.extensional_member (
    uuid uuid NOT NULL,
    code character varying NOT NULL,
    added_by character varying NOT NULL,
    vs_version_uuid uuid NOT NULL,
    terminology_version_uuid uuid NOT NULL,
    display character varying NOT NULL
);

CREATE TABLE value_sets.history (
    id integer NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    schema_name character varying,
    table_name character varying NOT NULL,
    operation character varying NOT NULL,
    who character varying,
    new_val json,
    old_val json
);

CREATE TABLE value_sets.mapping_inclusion (
    uuid uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    concept_map_uuid uuid,
    relationship_types character varying,
    match_source_or_target character varying NOT NULL,
    concept_map_name character varying,
    vs_version_uuid uuid NOT NULL
);

CREATE TABLE value_sets.value_set (
    uuid uuid NOT NULL,
    name character varying NOT NULL,
    title character varying,
    publisher character varying,
    contact character varying,
    description character varying,
    immutable boolean,
    experimental boolean,
    purpose character varying,
    type character varying,
    use_case_uuid uuid
);

CREATE TABLE value_sets.value_set_rule (
    uuid uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    "position" integer,
    description character varying NOT NULL,
    property character varying NOT NULL,
    operator character varying NOT NULL,
    value character varying NOT NULL,
    include boolean NOT NULL,
    value_set_version uuid NOT NULL,
    terminology_version uuid NOT NULL,
    rule_group integer
);

CREATE TABLE value_sets.value_set_version (
    uuid uuid NOT NULL,
    value_set_uuid uuid,
    status character varying,
    description character varying,
    created_date date DEFAULT now(),
    version integer NOT NULL,
    comments text,
    effective_start date,
    effective_end date
);

ALTER TABLE ONLY value_sets.expansion_member
    ADD CONSTRAINT expansion_member_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY value_sets.expansion
    ADD CONSTRAINT expansion_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY value_sets.explicitly_included_code
    ADD CONSTRAINT explicitly_included_code_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY value_sets.extensional_member
    ADD CONSTRAINT extensional_members_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY value_sets.mapping_inclusion
    ADD CONSTRAINT mapping_inclusion_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY value_sets.value_set
    ADD CONSTRAINT unique_name UNIQUE (name) INCLUDE (name);

ALTER TABLE ONLY value_sets.value_set
    ADD CONSTRAINT unique_uuid UNIQUE (uuid) INCLUDE (uuid);

ALTER TABLE ONLY value_sets.history
    ADD CONSTRAINT value_set_history_pkey PRIMARY KEY (id);

ALTER TABLE ONLY value_sets.value_set
    ADD CONSTRAINT value_set_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY value_sets.value_set_rule
    ADD CONSTRAINT value_set_rule_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY value_sets.value_set_version
    ADD CONSTRAINT value_set_version_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY value_sets.value_set_version
    ADD CONSTRAINT version_identifier UNIQUE (value_set_uuid, version) INCLUDE (value_set_uuid, version);

ALTER TABLE value_sets.value_set
    ADD CONSTRAINT vs_type CHECK (((type)::text = ANY ((ARRAY['intensional'::character varying, 'extensional'::character varying])::text[]))) NOT VALID;

ALTER TABLE ONLY value_sets.explicitly_included_code
    ADD CONSTRAINT code_fk FOREIGN KEY (code_uuid) REFERENCES custom_terminologies.code(uuid);

ALTER TABLE ONLY value_sets.expansion_member
    ADD CONSTRAINT expansion_fk FOREIGN KEY (expansion_uuid) REFERENCES value_sets.expansion(uuid);

ALTER TABLE ONLY value_sets.value_set_version
    ADD CONSTRAINT foreign_key FOREIGN KEY (value_set_uuid) REFERENCES value_sets.value_set(uuid) NOT VALID;

ALTER TABLE ONLY value_sets.mapping_inclusion
    ADD CONSTRAINT map_uuid_fk FOREIGN KEY (concept_map_uuid) REFERENCES concept_maps.concept_map(uuid);

ALTER TABLE ONLY value_sets.explicitly_included_code
    ADD CONSTRAINT review_status_fk FOREIGN KEY (review_status) REFERENCES project_management.status(display) NOT VALID;

ALTER TABLE ONLY value_sets.value_set_version
    ADD CONSTRAINT status_fk FOREIGN KEY (status) REFERENCES project_management.status(display) NOT VALID;

ALTER TABLE ONLY value_sets.value_set_rule
    ADD CONSTRAINT terminology_fk FOREIGN KEY (terminology_version) REFERENCES public.terminology_versions(uuid) NOT VALID;

ALTER TABLE ONLY value_sets.extensional_member
    ADD CONSTRAINT terminology_ver_fk FOREIGN KEY (terminology_version_uuid) REFERENCES public.terminology_versions(uuid) NOT VALID;

ALTER TABLE ONLY value_sets.value_set
    ADD CONSTRAINT use_case_fk FOREIGN KEY (use_case_uuid) REFERENCES project_management.use_case(uuid) NOT VALID;

ALTER TABLE ONLY value_sets.expansion
    ADD CONSTRAINT vs_ver_fk FOREIGN KEY (vs_version_uuid) REFERENCES value_sets.value_set_version(uuid) NOT VALID;

ALTER TABLE ONLY value_sets.mapping_inclusion
    ADD CONSTRAINT vs_ver_fk FOREIGN KEY (vs_version_uuid) REFERENCES value_sets.value_set_version(uuid) NOT VALID;

ALTER TABLE ONLY value_sets.value_set_rule
    ADD CONSTRAINT vs_version_fk FOREIGN KEY (value_set_version) REFERENCES value_sets.value_set_version(uuid);

ALTER TABLE ONLY value_sets.extensional_member
    ADD CONSTRAINT vs_version_fk FOREIGN KEY (vs_version_uuid) REFERENCES value_sets.value_set_version(uuid) NOT VALID;

ALTER TABLE ONLY value_sets.explicitly_included_code
    ADD CONSTRAINT vs_version_fk FOREIGN KEY (vs_version_uuid) REFERENCES value_sets.value_set_version(uuid) NOT VALID;
