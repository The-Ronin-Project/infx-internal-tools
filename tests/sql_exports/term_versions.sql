--
-- PostgreSQL database dump
--

-- Dumped from database version 11.11
-- Dumped by pg_dump version 13.3

-- Started on 2022-01-04 17:10:52 MST

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
-- TOC entry 269 (class 1259 OID 87669)
-- Name: terminology_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE terminology_versions (
    uuid uuid NOT NULL,
    terminology character varying NOT NULL,
    version character varying NOT NULL,
    effective_start date,
    effective_end date,
    fhir_uri character varying
);


--
-- TOC entry 4547 (class 0 OID 87669)
-- Dependencies: 269
-- Data for Name: terminology_versions; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO terminology_versions VALUES ('0191c3e4-215b-11ec-9621-0242ac130002', 'ICD-10 CM', '2021', NULL, NULL, 'http://hl7.org/fhir/sid/icd-10-cm');
INSERT INTO terminology_versions VALUES ('3b07a086-2227-11ec-9621-0242ac130002', 'SNOMED CT', '2021', NULL, NULL, 'http://snomed.info/sct');
INSERT INTO terminology_versions VALUES ('85d038ea-2857-11ec-9621-0242ac130002', 'RxNorm', '2021', NULL, NULL, 'http://www.nlm.nih.gov/research/umls/rxnorm');
INSERT INTO terminology_versions VALUES ('1ea19640-63e6-4e1b-b82f-be444ba395b4', 'ICD-10 CM', '2022', NULL, NULL, 'http://hl7.org/fhir/sid/icd-10-cm');
INSERT INTO terminology_versions VALUES ('6c6219c8-5ef3-11ec-8f16-acde48001122', 'CPT', '2022', '2022-01-01', '2022-12-31', 'http://www.ama-assn.org/go/cpt');
INSERT INTO terminology_versions VALUES ('bb8576cd-f1d4-4815-8609-d21b2444e6c1', 'HCPCS', 'Jan 2022', '2022-01-01', '2022-03-31', 'http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets');
INSERT INTO terminology_versions VALUES ('afb82f16-a129-40dc-bb3f-c7d05532f522', 'Value Sets', 'N/A', NULL, NULL, 'N/A');


-- Completed on 2022-01-04 17:10:59 MST

--
-- PostgreSQL database dump complete
--

