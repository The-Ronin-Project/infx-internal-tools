--
-- PostgreSQL database dump
--

-- Dumped from database version 11.16
-- Dumped by pg_dump version 14.1

-- Started on 2022-08-31 12:32:12 MDT

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
-- TOC entry 279 (class 1259 OID 87669)
-- Name: terminology_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE terminology_versions (
    uuid uuid NOT NULL,
    terminology character varying NOT NULL,
    version character varying NOT NULL,
    effective_start date,
    effective_end date,
    fhir_uri character varying,
    is_standard boolean DEFAULT false NOT NULL,
    fhir_terminology boolean DEFAULT false NOT NULL
);


--
-- TOC entry 4859 (class 0 OID 87669)
-- Dependencies: 279
-- Data for Name: terminology_versions; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO terminology_versions VALUES ('bb8576cd-f1d4-4815-8609-d21b2444e6c1', 'HCPCS', 'Jan 2022', '2022-01-01', '2022-03-31', 'http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets', false, false);
INSERT INTO terminology_versions VALUES ('f65f7163-3ca7-45ad-9391-f6fbbe400b6d', 'MDA FHIR Location', '1', '2022-01-01', NULL, 'http://projectronin.io/fhir/terminologies/MDAFHIRLocation', false, false);
INSERT INTO terminology_versions VALUES ('0191c3e4-215b-11ec-9621-0242ac130002', 'ICD-10 CM', '2021', NULL, NULL, 'http://hl7.org/fhir/sid/icd-10-cm', true, false);
INSERT INTO terminology_versions VALUES ('1ea19640-63e6-4e1b-b82f-be444ba395b4', 'ICD-10 CM', '2022', '2022-01-01', '2022-12-31', 'http://hl7.org/fhir/sid/icd-10-cm', true, false);
INSERT INTO terminology_versions VALUES ('3b07a086-2227-11ec-9621-0242ac130002', 'SNOMED CT', '2021-09-01', '2021-01-01', '2022-02-28', 'http://snomed.info/sct', true, false);
INSERT INTO terminology_versions VALUES ('5efa6244-32ad-4a2d-9d21-8f237499325a', 'SNOMED CT', '2022-03-01', '2022-03-01', NULL, 'http://snomed.info/sct', true, false);
INSERT INTO terminology_versions VALUES ('6c6219c8-5ef3-11ec-8f16-acde48001122', 'CPT', '2022', '2022-01-01', '2022-12-31', 'http://www.ama-assn.org/go/cpt', true, false);
INSERT INTO terminology_versions VALUES ('7c19e704-19d9-412b-90c3-79c5fb99ebe8', 'LOINC', '2.71', '2021-08-25', '2022-02-15', 'http://loinc.org', true, false);
INSERT INTO terminology_versions VALUES ('aca5b59a-9f1e-4946-8450-610df97b72a8', 'LOINC', '2.72', '2022-02-16', '2022-08-31', 'http://loinc.org', true, false);
INSERT INTO terminology_versions VALUES ('040e63dc-f406-4c10-8e9b-15bee64450e5', 'chokuto cancer types', '1', '2022-03-22', NULL, 'http://projectronin.io/fhir/terminologies/chokuto_cancer_types', false, false);
INSERT INTO terminology_versions VALUES ('0d6d34d8-3373-4999-9e9c-8c02e6fd3a45', 'MDA Lab Codes', '1', '2022-03-20', NULL, 'https://www.mdanderson.org/oid:1.2.840.114350.1.13.412.2.7.5.737384.134', false, false);
INSERT INTO terminology_versions VALUES ('db3d6526-5dce-4230-8ba9-1466ae00e75f', 'Project Ronin Surveys', '1', '2022-03-20', NULL, 'http://projectronin.io/fhir/terminologies/ProjectRoninSurveys', false, false);
INSERT INTO terminology_versions VALUES ('e3dbd59c-aa26-11ec-b909-0242ac120002', 'Project Ronin Value Sets', '1', '2022-03-20', NULL, 'http://projectronin.io/fhir/terminologies/ProjectRoninValueSets', false, false);
INSERT INTO terminology_versions VALUES ('de0e3b38-70cc-41c3-8388-5b155c3c42f7', 'NLP Symptoms Extraction Model', '1', '2022-04-01', NULL, 'http://projectronin.io/fhir/terminologies/NLPSymptomsExtractionModel', false, false);
INSERT INTO terminology_versions VALUES ('d903e26d-26a1-4668-989e-78e6722c6ed7', 'FHIR Appointment Status', '4.6.0', '2022-04-19', '2029-04-19', 'http://hl7.org/fhir/appointmentstatus', true, false);
INSERT INTO terminology_versions VALUES ('1cd9ae68-00f2-485c-b5d6-439ac606229e', 'Internal Relationship Codes', '1', '2022-04-01', NULL, 'http://projectronin.io/fhir/terminologies/InternalRelationshipCodes', false, false);
INSERT INTO terminology_versions VALUES ('afb82f16-a129-40dc-bb3f-c7d05532f522', 'Value Sets', 'N/A', '2022-03-20', NULL, 'http://projectronin.io/fhir/terminologies/ValueSets', false, false);
INSERT INTO terminology_versions VALUES ('60f15a17-973e-4987-ad71-22777eac994a', 'ICD-10 PCS', '2022', '2022-01-01', '2022-12-31', 'http://hl7.org/fhir/sid/icd-10-pcs', false, false);
INSERT INTO terminology_versions VALUES ('0166f620-ddd8-11ec-9d64-0242ac120002', 'HCPCS', 'July 2022', '2022-07-01', NULL, 'http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets', true, false);
INSERT INTO terminology_versions VALUES ('f1fc2d83-87d5-4761-a2c1-c5df33211361', 'ronin_psj_contact_point_use', '1', '2022-06-07', '2022-06-07', 'http://projectronin.io/fhir/terminologies/ronin_psj_contact_point_use', false, false);
INSERT INTO terminology_versions VALUES ('e601e5f7-450f-43c2-8b9f-a3db7c583620', 'INFX Project Management Status', '1', '2022-06-07', '2022-06-07', 'http://projectronin.io/fhir/terminologies/INFXProjectManagementStatus', true, false);
INSERT INTO terminology_versions VALUES ('85d038ea-2857-11ec-9621-0242ac130002', 'RxNorm', '04-Apr-2022', NULL, NULL, 'http://www.nlm.nih.gov/research/umls/rxnorm', true, false);
INSERT INTO terminology_versions VALUES ('08a465ee-d138-4ee1-a94e-8dc7a6a8ed7b', 'MDA drug codes', '1', '2022-07-25', '2023-07-25', 'https://www.mdanderson.org/oid:1.2.840.114350.1.13.412.2.7.5.737384.134', false, false);
INSERT INTO terminology_versions VALUES ('3c9ed300-0cb8-47af-8c04-a06352a14b8d', 'test', '1', '2022-08-26', '2022-09-19', 'http://test_test.com', false, false);
INSERT INTO terminology_versions VALUES ('6b435375-7c90-41ba-82d3-3a5e4037a958', 'FHIR\PublicationStatus', '4.6.0', '2022-03-20', NULL, 'http://hl7.org/fhir/publication-status', false, true);
INSERT INTO terminology_versions VALUES ('3a95d199-01a2-426e-b2b5-6dcdb5ff49f5', 'HL7Workgroup', '4.6.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/hl7-work-group', true, true);
INSERT INTO terminology_versions VALUES ('8557d482-5ef8-4eb4-bcc8-46b9d22c1321', 'AmericanIndianAlaskaNativeLanguages', '2.1.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v3-AmericanIndianAlaskaNativeLanguages', true, true);
INSERT INTO terminology_versions VALUES ('c8506208-82eb-4af7-bd3d-0c01c57f035b', 'MaritalStatus', '2.1.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v3-MaritalStatus', true, true);
INSERT INTO terminology_versions VALUES ('161def11-beac-4467-8bbf-eaa6d2c5add2', 'PostalAddressUse', '2.0.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v3-PostalAddressUse', true, true);
INSERT INTO terminology_versions VALUES ('e0f6fa50-d517-4ff2-a0ec-e33ce1c126ab', 'TelecommunicationAddressUse', '2.0.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v3-TelecommunicationAddressUse', true, true);
INSERT INTO terminology_versions VALUES ('51c9ad51-7dbb-436a-962a-9c9ccce161d5', 'TelecommunicationCapabilities', '2.1.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v3-TelecommunicationCapabilities', true, true);
INSERT INTO terminology_versions VALUES ('31350c5a-0a29-4ee9-9020-c5e4cc332e89', 'AppointmentStatus', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/appointmentstatus', true, true);
INSERT INTO terminology_versions VALUES ('baad4526-96a6-4686-a2cb-92c1fc9d3d62', 'appointmentReason', '2.1.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v2-0276', true, true);
INSERT INTO terminology_versions VALUES ('a7cf79e7-49bf-4423-8ba9-d629f4b1d770', 'IdentifierUse', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/identifier-use', true, true);
INSERT INTO terminology_versions VALUES ('a17f96a3-fba0-462d-b4cc-6d1ae9152839', 'Participant type', '0.5.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/participant-type', true, true);
INSERT INTO terminology_versions VALUES ('532f76d3-833c-49ae-989b-74b9f620c8c1', 'Service category', '0.1.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/service-category', true, true);
INSERT INTO terminology_versions VALUES ('e00bd044-0e4b-44d6-9879-220f95b9db0d', 'contactRole2', '2.1.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v2-0131', true, true);
INSERT INTO terminology_versions VALUES ('55a68656-d7ea-40c4-8703-7ff6f6dc645e', 'telecommunicationEquipmentType', '2.3.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v2-0202', true, true);
INSERT INTO terminology_versions VALUES ('eb272cef-b694-411a-95ee-af60fe752587', 'identifierType', '2.9.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v2-0203', true, true);
INSERT INTO terminology_versions VALUES ('ec278378-8fa8-450c-9f9e-5b58c9d1f70c', 'degreeLicenseCertificate', '2.1.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v2-0360', true, true);
INSERT INTO terminology_versions VALUES ('c7fb5127-7d1a-4a2d-ba37-0515879f9a07', 'Condition Category Codes', '4.3.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/condition-category', true, true);
INSERT INTO terminology_versions VALUES ('9889bc56-a89a-4e4d-abec-30d5e00db3f0', 'Condition Clinical Status Codes', '4.0.1', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/condition-clinical', true, true);
INSERT INTO terminology_versions VALUES ('3ea6fb94-b400-4fe4-b8e1-59aa3b223cb0', 'ConditionVerificationStatus', '4.0.1', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/condition-ver-status', true, true);
INSERT INTO terminology_versions VALUES ('38cedbd7-9464-4a7f-9a3c-abfecc6a5b63', 'NamingSystemIdentifierType', '4.3.0', NULL, NULL, 'http://hl7.org/fhir/namingsystem-identifier-type', true, true);
INSERT INTO terminology_versions VALUES ('07f7df50-7d25-4caf-beba-4c6d32ee57db', 'NarrativeStatus', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/narrative-status', true, true);
INSERT INTO terminology_versions VALUES ('fdde0d46-9751-4b89-9e6d-f9007637c9df', 'ParticipantRequired', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/participantrequired', true, true);
INSERT INTO terminology_versions VALUES ('1450b331-f6f2-4fa9-8777-c9b29fc4f23a', 'ParticipationStatus', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/participationstatus', true, true);
INSERT INTO terminology_versions VALUES ('5c6ee4b8-d6c5-4976-ae31-38ee13432c84', 'Practitioner specialty', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/practitioner-specialty', true, true);
INSERT INTO terminology_versions VALUES ('0d50a14b-134c-4237-a9fc-84491ac69454', 'RequestIntent', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/request-intent', true, true);
INSERT INTO terminology_versions VALUES ('ffcb0799-f174-4b0a-b90d-a2ae0e37caac', 'RequestStatus', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/request-status', true, true);
INSERT INTO terminology_versions VALUES ('f73d6eb4-6d6a-472d-8445-2f12c49bf0c7', 'Service type', '4.0.1', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/service-type', true, true);
INSERT INTO terminology_versions VALUES ('bc5112dc-e91e-41d3-851f-4e4883da7a7c', 'EntityNameUse', '2.1.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v3-EntityNameUse', true, true);
INSERT INTO terminology_versions VALUES ('3725d8b2-145a-4daf-ba1e-4757fb390085', 'Appointment cancellation reason', '0.1.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/appointment-cancellation-reason', true, true);
INSERT INTO terminology_versions VALUES ('0218d868-7a14-4bcf-9925-526c1287a64c', 'Ethnicity', '2.1.0', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v3-Ethnicity', true, true);
INSERT INTO terminology_versions VALUES ('da7761cc-8b3f-4f33-a161-4cc28e983d60', 'AddressType', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/address-type', true, true);
INSERT INTO terminology_versions VALUES ('db88642a-1e5f-4a15-a280-f284a21fa58b', 'AddressUse', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/address-use', true, true);
INSERT INTO terminology_versions VALUES ('9699f696-463c-463f-9212-950abe6b4fd3', 'AdministrativeGender', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/administrative-gender', true, true);
INSERT INTO terminology_versions VALUES ('dc87d70d-affc-4cb0-84b6-0db9a058a413', 'ContactPointSystem', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/contact-point-system', true, true);
INSERT INTO terminology_versions VALUES ('5026c128-0349-4a9d-9806-13c70d1d5460', 'ContactPointUse', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/contact-point-use', true, true);
INSERT INTO terminology_versions VALUES ('175ebf8c-1eff-4ac9-9bd5-c5a6c3f83999', 'v3 Code System NullFlavor', '2018-08-12', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v3-NullFlavor', true, true);
INSERT INTO terminology_versions VALUES ('ce6542f4-40cd-4647-8abc-cb425202a0f7', 'v3 Code System Race', '2018-08-12', NULL, NULL, 'http://terminology.hl7.org/CodeSystem/v3-Race', true, true);
INSERT INTO terminology_versions VALUES ('30e9ff67-68ae-4ce8-8fad-e2fd7fdd91c0', 'NameUse', '4.0.1', NULL, NULL, 'http://hl7.org/fhir/name-use', true, true);
INSERT INTO terminology_versions VALUES ('73e89db8-fbcf-4786-a16d-a83693f29024', 'LOINC', '2.73', '2022-08-08', '2023-02-14', 'http://loinc.org', true, false);
INSERT INTO terminology_versions VALUES ('011497ab-1092-46c5-b66a-95e4acef599b', 'Test Bug', '1.0', '2022-08-24', '2022-08-25', 'http://projectronin.io/fhir/terminologies/ProjectRoninValueSets', false, false);
INSERT INTO terminology_versions VALUES ('fc98b31d-c83e-48c0-be96-2d55661219af', 'MDA drug displays', '1.0.0', '2022-08-31', '2023-08-31', 'https://www.mdanderson.org/oid:1.2.840.114350.1.13.412.2.7.5.737384.134', false, false);


-- Completed on 2022-08-31 12:32:17 MDT

--
-- PostgreSQL database dump complete
--

