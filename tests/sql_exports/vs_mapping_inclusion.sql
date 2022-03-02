--
-- PostgreSQL database dump
--

-- Dumped from database version 11.12
-- Dumped by pg_dump version 14.1

-- Started on 2022-03-02 14:02:13 MST

-- SET statement_timeout = 0;
-- SET lock_timeout = 0;
-- SET idle_in_transaction_session_timeout = 0;
-- SET client_encoding = 'UTF8';
-- SET standard_conforming_strings = on;
-- SELECT pg_catalog.set_config('search_path', '', false);
-- SET check_function_bodies = false;
-- SET xmloption = content;
-- SET client_min_messages = warning;
-- SET row_security = off;

--
-- TOC entry 617 (class 1259 OID 165463)
-- Name: mapping_inclusion; Type: TABLE; Schema: value_sets; Owner: -
--

CREATE TABLE value_sets.mapping_inclusion (
    uuid uuid NOT NULL,
    concept_map_uuid uuid,
    relationship_types character varying,
    match_source_or_target character varying NOT NULL,
    concept_map_name character varying,
    vs_version_uuid uuid NOT NULL
);


--
-- TOC entry 5749 (class 0 OID 165463)
-- Dependencies: 617
-- Data for Name: mapping_inclusion; Type: TABLE DATA; Schema: value_sets; Owner: -
--

-- INSERT INTO value_sets.mapping_inclusion VALUES ('6768a0c0-9a64-11ec-ab72-7f6dedcfad4b', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7db2688a-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('09094d20-98e5-11ec-9f68-db9c87d2cb82', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '50897740-8a91-11ec-a3a5-2d995c1e782a');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('0b948f10-98e9-11ec-874b-bb6abc689c71', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7e447ebe-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('18c8af90-98e9-11ec-ada7-31a5c412e8e7', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7d80924c-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('2033a460-98e9-11ec-9cc5-bd24a3df173d', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7f437ee6-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('267ef860-98e9-11ec-8e7f-0b2664b8c29c', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7e74be62-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('2c55b8a0-98e9-11ec-9261-bf1745badcde', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7e848aea-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('31c552a0-98e9-11ec-93d6-2f889e084465', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7e947b44-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('3780e9c0-98e9-11ec-8604-d54ef0f6134a', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7ea4657c-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('3e966b90-98e9-11ec-a09c-61e67135d953', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7eb452f2-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('4c6d82d0-98e9-11ec-8ba4-3df300a06e6c', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7ec4879e-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('5c05f920-98e9-11ec-bad8-0be346879fbd', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7ed45f70-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('61ca90f0-98e9-11ec-a8e5-b3db4d6f4c7e', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7ee43b2a-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('6860d0a0-98e9-11ec-b480-f76b2db6fbd9', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7ef40eba-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('6ef05990-98e9-11ec-b057-47e0a2b046e6', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7f03deda-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('76359df0-98e9-11ec-80ed-15c00035a4c6', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7f13e1ae-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('7e11a410-98e9-11ec-b5c0-577fce6f8463', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7f239ad6-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('85cbca50-98e9-11ec-b537-173270e48cb7', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7f336024-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('8e9707e0-98e8-11ec-a359-89b2b39c905a', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7d9153de-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('97c3d0f0-98e8-11ec-9aa3-078978120610', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7da17c28-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('a82f0090-98e8-11ec-bb53-095ec5aa7380', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7dc239d6-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('afd095c0-98e8-11ec-817d-0d831b5c9dc9', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7dd20d0c-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('b8315c40-98e8-11ec-86cc-e32e0121a06a', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7f536374-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('c0f0bd80-98e8-11ec-84c9-93a212e98dda', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7de1d412-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('cc13d850-98e8-11ec-8ec7-8da74425d463', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7df1b896-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('d4228730-98e8-11ec-b7a7-e7080dd66ddb', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7e025160-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('dd2c8600-98e8-11ec-b7fb-4b7b691cceae', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7e1367de-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('e7cb55a0-98e8-11ec-acd7-9b5689274791', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7e244c98-94f0-11ec-b0b2-00163e1124e7');
-- INSERT INTO value_sets.mapping_inclusion VALUES ('f242c720-98e8-11ec-aa4e-516a414a3b0d', NULL, '{"wider","relatedto","equivalent","narrower","equal"}', 'target', 'MDA to LOINC - Katelin', '7d310efc-94f0-11ec-b0b2-00163e1124e7');


-- Completed on 2022-03-02 14:02:18 MST

--
-- PostgreSQL database dump complete
--

