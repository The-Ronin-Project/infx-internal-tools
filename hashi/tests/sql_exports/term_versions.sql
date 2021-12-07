--
-- PostgreSQL database dump
--

-- Dumped from database version 11.11
-- Dumped by pg_dump version 13.3

-- Started on 2021-10-19 11:44:22 MDT

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
-- TOC entry 4491 (class 0 OID 87669)
-- Dependencies: 269
-- Data for Name: terminology_versions; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO terminology_versions VALUES ('0191c3e4-215b-11ec-9621-0242ac130002', 'ICD-10 CM', '2021', NULL, NULL, 'http://hl7.org/fhir/sid/icd-10-cm');
INSERT INTO terminology_versions VALUES ('3b07a086-2227-11ec-9621-0242ac130002', 'SNOMED CT', '2021', NULL, NULL, 'http://snomed.info/sct');
INSERT INTO terminology_versions VALUES ('85d038ea-2857-11ec-9621-0242ac130002', 'RxNorm', '2021', NULL, NULL, 'http://www.nlm.nih.gov/research/umls/rxnorm');


--
-- TOC entry 4367 (class 2606 OID 87676)
-- Name: terminology_versions terminology_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

-- ALTER TABLE ONLY terminology_versions
--     ADD CONSTRAINT terminology_versions_pkey PRIMARY KEY (uuid);


-- Completed on 2021-10-19 11:44:25 MDT

--
-- PostgreSQL database dump complete
--

