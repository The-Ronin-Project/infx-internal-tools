--
-- PostgreSQL database dump
--

-- Dumped from database version 11.12
-- Dumped by pg_dump version 14.1

-- Started on 2022-05-10 18:46:36 MDT

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
-- TOC entry 274 (class 1259 OID 87669)
-- Name: terminology_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE terminology_versions (
    uuid uuid NOT NULL,
    terminology character varying NOT NULL,
    version character varying NOT NULL,
    effective_start date,
    effective_end date,
    fhir_uri character varying,
    is_standard boolean DEFAULT false NOT NULL
);


--
-- TOC entry 5863 (class 0 OID 87669)
-- Dependencies: 274
-- Data for Name: terminology_versions; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO terminology_versions VALUES ('bb8576cd-f1d4-4815-8609-d21b2444e6c1', 'HCPCS', 'Jan 2022', '2022-01-01', '2022-03-31', 'http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets', false);
INSERT INTO terminology_versions VALUES ('f65f7163-3ca7-45ad-9391-f6fbbe400b6d', 'MDA FHIR Location', '1', '2022-01-01', NULL, 'http://projectronin.io/fhir/terminologies/MDAFHIRLocation', false);
INSERT INTO terminology_versions VALUES ('0191c3e4-215b-11ec-9621-0242ac130002', 'ICD-10 CM', '2021', NULL, NULL, 'http://hl7.org/fhir/sid/icd-10-cm', true);
INSERT INTO terminology_versions VALUES ('1ea19640-63e6-4e1b-b82f-be444ba395b4', 'ICD-10 CM', '2022', '2022-01-01', '2022-12-31', 'http://hl7.org/fhir/sid/icd-10-cm', true);
INSERT INTO terminology_versions VALUES ('3b07a086-2227-11ec-9621-0242ac130002', 'SNOMED CT', '2021-09-01', '2021-01-01', '2022-02-28', 'http://snomed.info/sct', true);
INSERT INTO terminology_versions VALUES ('5efa6244-32ad-4a2d-9d21-8f237499325a', 'SNOMED CT', '2022-03-01', '2022-03-01', NULL, 'http://snomed.info/sct', true);
INSERT INTO terminology_versions VALUES ('6c6219c8-5ef3-11ec-8f16-acde48001122', 'CPT', '2022', '2022-01-01', '2022-12-31', 'http://www.ama-assn.org/go/cpt', true);
INSERT INTO terminology_versions VALUES ('7c19e704-19d9-412b-90c3-79c5fb99ebe8', 'LOINC', '2.71', '2021-08-25', '2022-02-15', 'http://loinc.org', true);
INSERT INTO terminology_versions VALUES ('85d038ea-2857-11ec-9621-0242ac130002', 'RxNorm', '2021', NULL, NULL, 'http://www.nlm.nih.gov/research/umls/rxnorm', true);
INSERT INTO terminology_versions VALUES ('aca5b59a-9f1e-4946-8450-610df97b72a8', 'LOINC', '2.72', '2022-02-16', '2022-08-31', 'http://loinc.org', true);
INSERT INTO terminology_versions VALUES ('040e63dc-f406-4c10-8e9b-15bee64450e5', 'chokuto cancer types', '1', '2022-03-22', NULL, 'http://projectronin.io/fhir/terminologies/chokuto_cancer_types', false);
INSERT INTO terminology_versions VALUES ('0d6d34d8-3373-4999-9e9c-8c02e6fd3a45', 'MDA Lab Codes', '1', '2022-03-20', NULL, 'https://www.mdanderson.org/oid:1.2.840.114350.1.13.412.2.7.5.737384.134', false);
INSERT INTO terminology_versions VALUES ('6b435375-7c90-41ba-82d3-3a5e4037a958', 'FHIR\PublicationStatus', '4.6.0', '2022-03-20', NULL, 'http://hl7.org/fhir/publication-status', false);
INSERT INTO terminology_versions VALUES ('db3d6526-5dce-4230-8ba9-1466ae00e75f', 'Project Ronin Surveys', '1', '2022-03-20', NULL, 'http://projectronin.io/fhir/terminologies/ProjectRoninSurveys', false);
INSERT INTO terminology_versions VALUES ('e3dbd59c-aa26-11ec-b909-0242ac120002', 'Project Ronin Value Sets', '1', '2022-03-20', NULL, 'http://projectronin.io/fhir/terminologies/ProjectRoninValueSets', false);
INSERT INTO terminology_versions VALUES ('28ebae40-0b1e-48cb-9139-3192d3fee8d7', 'FHIR\ContactPointUse', '4.0.1', '2019-11-01', NULL, 'http://hl7.org/fhir/contact-point-use', false);
INSERT INTO terminology_versions VALUES ('1746af58-fc64-4e3a-a7ab-62afacb0604c', 'test', 'test', '2022-02-07', '2022-02-08', '', false);
INSERT INTO terminology_versions VALUES ('de0e3b38-70cc-41c3-8388-5b155c3c42f7', 'NLP Symptoms Extraction Model', '1', '2022-04-01', NULL, 'http://projectronin.io/fhir/terminologies/NLPSymptomsExtractionModel', false);
INSERT INTO terminology_versions VALUES ('4eb1de03-d506-4419-bb6a-e6386b3e74f6', 'FHIR\ContactPointSystem', '4.6.0', '2022-04-14', NULL, 'http://hl7.org/fhir/contact-point-system', false);
INSERT INTO terminology_versions VALUES ('d903e26d-26a1-4668-989e-78e6722c6ed7', 'FHIR Appointment Status', '4.6.0', '2022-04-19', '2029-04-19', 'http://hl7.org/fhir/appointmentstatus', true);
INSERT INTO terminology_versions VALUES ('1cd9ae68-00f2-485c-b5d6-439ac606229e', 'Internal Relationship Codes', '1', '2022-04-01', NULL, 'http://projectronin.io/fhir/terminologies/InternalRelationshipCodes', false);
INSERT INTO terminology_versions VALUES ('afb82f16-a129-40dc-bb3f-c7d05532f522', 'Value Sets', 'N/A', '2022-03-20', NULL, 'http://projectronin.io/fhir/terminologies/ValueSets', false);
INSERT INTO terminology_versions VALUES ('60f15a17-973e-4987-ad71-22777eac994a', 'ICD-10 PCS', '2022', '2022-01-01', '2022-12-31', 'http://hl7.org/fhir/sid/icd-10-pcs', false);


-- Completed on 2022-05-10 18:46:41 MDT

--
-- PostgreSQL database dump complete
--

