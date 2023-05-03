
CREATE SCHEMA concept_maps;

CREATE TABLE concept_maps.concept_map (
    uuid uuid NOT NULL,
    name character varying,
    title character varying,
    publisher character varying,
    author character varying,
    purpose character varying,
    description character varying,
    created_date character varying,
    experimental boolean,
    include_self_map boolean,
    source_value_set_uuid uuid,
    target_value_set_uuid uuid,
    use_case_uuid uuid,
    auto_advance_mapping boolean DEFAULT false NOT NULL,
    auto_fill_search boolean DEFAULT false NOT NULL,
    show_target_codes boolean DEFAULT false NOT NULL
);

CREATE TABLE concept_maps.concept_map_version (
    concept_map_uuid uuid NOT NULL,
    uuid uuid NOT NULL,
    description character varying,
    comments character varying,
    status character varying,
    created_date timestamp with time zone,
    version integer,
    published_date timestamp with time zone,
    source_value_set_version_uuid uuid,
    target_value_set_version_uuid uuid
);

CREATE TABLE concept_maps.concept_map_version_terminologies (
    concept_map_version_uuid uuid,
    terminology_version_uuid uuid,
    context character varying,
    special_use_enabled boolean,
    uuid uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    value_set_uuid uuid
);

CREATE TABLE concept_maps.concept_relationship (
    concept_map_version_uuid uuid,
    review_status character varying,
    mapping_comments character varying,
    uuid uuid NOT NULL,
    target_concept_code character varying,
    target_concept_display character varying,
    target_concept_system character varying,
    created_date timestamp with time zone,
    reviewed_date timestamp with time zone,
    author character varying,
    source_concept_uuid uuid NOT NULL,
    relationship_code_uuid uuid,
    target_concept_system_version_uuid uuid,
    review_comment character varying,
    reviewed_by character varying
);

CREATE TABLE concept_maps.history (
    id integer DEFAULT nextval('concept_maps.cm_history_seq'::regclass) NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    schema_name character varying,
    table_name character varying NOT NULL,
    operation character varying NOT NULL,
    who character varying,
    new_val json,
    old_val json
);

CREATE TABLE concept_maps.relationship_codes (
    uuid uuid NOT NULL,
    code character varying,
    display character varying,
    system character varying,
    additional_context character varying,
    special_use boolean,
    relationship_system_uuid uuid
);

CREATE TABLE concept_maps.relationship_system (
    uuid uuid NOT NULL,
    name character varying,
    url character varying
);

CREATE TABLE concept_maps.source_concept (
    uuid uuid NOT NULL,
    code character varying,
    display character varying,
    system character varying,
    comments character varying,
    additional_context character varying,
    map_status character varying,
    concept_map_version_uuid uuid,
    assigned_mapper uuid,
    assigned_reviewer uuid,
    no_map boolean,
    reason_for_no_map character varying,
    mapping_group character varying,
    resource_count bigint
);

CREATE TABLE concept_maps.source_concept2 (
    uuid uuid NOT NULL,
    code character varying,
    display character varying,
    system character varying,
    comments character varying,
    additional_context character varying,
    map_status character varying,
    concept_map_version_uuid uuid,
    assigned_mapper uuid,
    assigned_reviewer uuid,
    no_map boolean,
    reason_for_no_map character varying,
    mapping_group character varying
);

CREATE TABLE concept_maps.source_display_frequency (
    instance_count bigint,
    patient_count bigint,
    display character varying
);

CREATE TABLE concept_maps.suggestion (
    uuid uuid NOT NULL,
    source_concept_uuid uuid NOT NULL,
    code character varying,
    display character varying,
    suggestion_source character varying,
    confidence numeric,
    "timestamp" timestamp with time zone DEFAULT now(),
    accepted boolean,
    terminology_version uuid,
    additional_info json
);

ALTER TABLE ONLY concept_maps.concept_map
    ADD CONSTRAINT concept_map_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY concept_maps.concept_map_version
    ADD CONSTRAINT concept_map_version_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY concept_maps.concept_map_version_terminologies
    ADD CONSTRAINT concept_map_version_terminologies_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY concept_maps.concept_relationship
    ADD CONSTRAINT concept_relationship_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY concept_maps.history
    ADD CONSTRAINT history_pkey PRIMARY KEY (id);

ALTER TABLE ONLY concept_maps.relationship_codes
    ADD CONSTRAINT relationship_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY concept_maps.relationship_system
    ADD CONSTRAINT relationship_system_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY concept_maps.source_concept2
    ADD CONSTRAINT source_concept2_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY concept_maps.source_concept
    ADD CONSTRAINT source_concept_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY concept_maps.suggestion
    ADD CONSTRAINT suggestion_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY concept_maps.concept_map_version
    ADD CONSTRAINT concept_map_uuid FOREIGN KEY (concept_map_uuid) REFERENCES concept_maps.concept_map(uuid) NOT VALID;

ALTER TABLE ONLY concept_maps.source_concept
    ADD CONSTRAINT concept_map_version FOREIGN KEY (concept_map_version_uuid) REFERENCES concept_maps.concept_map_version(uuid) NOT VALID;

ALTER TABLE ONLY concept_maps.source_concept2
    ADD CONSTRAINT concept_map_version FOREIGN KEY (concept_map_version_uuid) REFERENCES concept_maps.concept_map_version(uuid);

ALTER TABLE ONLY concept_maps.concept_relationship
    ADD CONSTRAINT concept_map_version_uuid FOREIGN KEY (concept_map_version_uuid) REFERENCES concept_maps.concept_map_version(uuid) NOT VALID;

ALTER TABLE ONLY concept_maps.concept_relationship
    ADD CONSTRAINT review_status FOREIGN KEY (review_status) REFERENCES project_management.status(display) NOT VALID;

ALTER TABLE ONLY concept_maps.suggestion
    ADD CONSTRAINT source_concept_fk FOREIGN KEY (source_concept_uuid) REFERENCES concept_maps.source_concept(uuid) NOT VALID;

ALTER TABLE ONLY concept_maps.concept_relationship
    ADD CONSTRAINT source_concept_uuid_fk FOREIGN KEY (source_concept_uuid) REFERENCES concept_maps.source_concept(uuid) NOT VALID;

ALTER TABLE ONLY concept_maps.concept_map
    ADD CONSTRAINT source_vs_fk FOREIGN KEY (source_value_set_uuid) REFERENCES value_sets.value_set(uuid) NOT VALID;

ALTER TABLE ONLY concept_maps.concept_map_version
    ADD CONSTRAINT source_vs_version FOREIGN KEY (source_value_set_version_uuid) REFERENCES value_sets.value_set_version(uuid) NOT VALID;

ALTER TABLE ONLY concept_maps.concept_map
    ADD CONSTRAINT target_vs_fk FOREIGN KEY (target_value_set_uuid) REFERENCES value_sets.value_set(uuid) NOT VALID;

ALTER TABLE ONLY concept_maps.concept_map_version
    ADD CONSTRAINT target_vs_version FOREIGN KEY (target_value_set_version_uuid) REFERENCES value_sets.value_set_version(uuid) NOT VALID;

ALTER TABLE ONLY concept_maps.suggestion
    ADD CONSTRAINT terminology_version_fk FOREIGN KEY (terminology_version) REFERENCES public.terminology_versions(uuid) NOT VALID;