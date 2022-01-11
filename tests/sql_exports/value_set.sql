--
-- PostgreSQL database dump
--

-- Dumped from database version 11.11
-- Dumped by pg_dump version 13.3

-- Started on 2022-01-11 15:58:33 MST

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
-- TOC entry 244 (class 1259 OID 56704)
-- Name: value_set; Type: TABLE; Schema: value_sets; Owner: -
--

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
    type character varying
);


--
-- TOC entry 4553 (class 0 OID 56704)
-- Dependencies: 244
-- Data for Name: value_set; Type: TABLE DATA; Schema: value_sets; Owner: -
--

INSERT INTO value_sets.value_set VALUES ('e1ba9050-4c8e-11ec-99d0-6d735801ea9a', 'Bladder Cancer Combined', 'bladder-cancer-combined', 'Project Ronin', 'Hao Sun', 'This valueset contains the diagnosis codes for bladder cancer and diseases that are clinically treated as bladder cancer. It contains both ICD-10 and SNOMED.', false, false, 'This valueset is multipurpose and is used in the ED Risk model, BI patient categorization and to identify patients for implementation purposes.', 'intensional');
INSERT INTO value_sets.value_set VALUES ('18550ff0-6364-11ec-bee7-1b2cefdcd5ad', 'Protein Kinase Inhibitors', 'Protein Kinase Inhibitors', 'Project Ronin', 'Hao Sun', 'RxNorm drugs, ingredients of protein kinase inhibitors ', false, false, 'This valueset will be used for ED prediction models, categorization of patients', 'intensional');
INSERT INTO value_sets.value_set VALUES ('ca1f9350-6cff-11ec-a5f0-91e137910fd5', 'AFP', 'AFP', 'Project Ronin', 'Stephen Weaver', 'Alpha-1 Fetoprotein LOINC Codes', false, false, 'ED Risk Model Lab Features', 'intensional');
INSERT INTO value_sets.value_set VALUES ('11192c20-6d01-11ec-a5f0-91e137910fd5', 'CA27.29', 'CA27.29', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating CA27.29 measurement', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('b070fe00-6d02-11ec-a5f0-91e137910fd5', 'CADM70K', 'CADM70K', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating Cancer Ag DM/70K measurement', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('c4f97e10-6d02-11ec-a5f0-91e137910fd5', 'CA549', 'CA549', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating CA549 measurement', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('ea625dc0-6d02-11ec-a5f0-91e137910fd5', 'CA242', 'CA242', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of CA242', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('55ce84d0-6d03-11ec-a5f0-91e137910fd5', 'CA50', 'CA50', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of CA50', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('d6d2c730-6d03-11ec-a5f0-91e137910fd5', 'Glucose', 'Glucose', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of Glucose', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('2b6f9c90-636e-11ec-a609-b5614185f7a5', 'Ureter Cancer Combined', 'Ureter Cancer Combined', 'Project Ronin', 'Katelin Brown', 'This valueset contains the diagnosis codes for ureter cancer and diseases that are clinically treated as ureter cancer. It contains both ICD-10 and SNOMED.', false, false, 'This valueset is multipurpose and is used in the ED Risk model, BI patient categorization and to identify patients for implementation purposes.', 'intensional');
INSERT INTO value_sets.value_set VALUES ('abd4ff40-6410-11ec-8daa-b365d151d4db', 'Cytotoxic Antibiotics and Related', 'Cytotoxic Antibiotics and Related', 'Project Ronin', 'Katelin', 'RxNorm drugs, ingredients of Cytotoxic Antibiotics and Related', false, false, 'This valueset will be used for ED prediction models, categorization of patients', 'intensional');
INSERT INTO value_sets.value_set VALUES ('98b92730-6413-11ec-9cfa-b7767fcb0438', 'Alkylating Agents', 'Alkylating Agents', 'Project Ronin', 'Katelin', 'RxNorm drugs, ingredients of Alkylating Agents and Related', false, false, 'This valueset will be used for ED prediction models, categorization of patients', 'intensional');
INSERT INTO value_sets.value_set VALUES ('089e1c50-6d00-11ec-a5f0-91e137910fd5', 'Albumin', 'Albumin', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating serum albumin measurements', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('245ec81a-2857-11ec-9621-0242ac130002', 'rxnorm-test', 'RxNorm Test', 'Project Ronin', 'Rey and Hao', 'a test value set', false, true, 'testing', 'intensional');
INSERT INTO value_sets.value_set VALUES ('1fe176a0-6d00-11ec-a5f0-91e137910fd5', 'Alkaline Phosphatase', 'Alkaline Phosphatase', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating serum Alkaline Phosphatase measurement', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('3c5e8de0-6d00-11ec-a5f0-91e137910fd5', 'ALT', 'ALT', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating serum ALT measurement', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('53a0afb0-6d00-11ec-a5f0-91e137910fd5', 'Amylase', 'Amylase', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating serum Amylase measurement', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('6c4d0fe0-6d00-11ec-a5f0-91e137910fd5', 'ANC', 'ANC', 'Project Ronin', 'Stephen Weaverr', 'LOINC codes indicating ANC measurement', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('51801230-52ec-11ec-89f4-eb5726153df6', 'Kidney Cancer Combined', 'Kidney Cancer Combined', 'Project Ronin', 'Stephen Weaver', 'This valueset contains the diagnosis codes for kidney cancer and diseases that are clinically treated as kidney cancer. It contains both ICD-10 and SNOMED.', false, false, 'This valueset is multipurpose and is used in the ED Risk model, BI patient categorization and to identify patients for implementation purposes.', 'intensional');
INSERT INTO value_sets.value_set VALUES ('035ba930-59ee-11ec-be32-7929feea4490', 'Prostate Cancer Combined', 'Prostate Cancer Combined', 'Project Ronin', 'Hao Sun', 'This valueset contains the diagnosis codes for prostate cancer and diseases that are clinically treated as prostate cancer. It contains both ICD-10 and SNOMED.', false, false, 'This valueset is multipurpose and is used in the ED Risk model, BI patient categorization and to identify patients for implementation purposes.', 'intensional');
INSERT INTO value_sets.value_set VALUES ('bfcb8eb0-6343-11ec-bd13-cbbf4db9fbeb', 'breast-cancer', 'breast-cancer', 'Project Ronin', 'Stephen Weaver', 'breast cancer codes, not mets to the breast, not skin cancer', false, false, 'This valueset is used to aggregate data for ED Risk Prediction feature ''Cancer Type-Breast Cancer''.', 'intensional');
INSERT INTO value_sets.value_set VALUES ('50ba21d0-634d-11ec-8615-fb597cd13462', 'Testicular Cancer Combined', 'Testicular Cancer Combined', 'Project Ronin', 'Hao Sun', 'This valueset contains the diagnosis codes for testicular cancer and diseases that are clinically treated as testicular cancer. It contains both ICD-10 and SNOMED.', false, false, 'This valueset is multipurpose and is used in the ED Risk model, BI patient categorization and to identify patients for implementation purposes.', 'intensional');
INSERT INTO value_sets.value_set VALUES ('6bda8080-6353-11ec-a426-01ccf2bd9dab', 'Urethral Cancer Combined', 'Urethral Cancer Combined', 'Project Ronin', 'Katelin', 'This valueset contains the diagnosis codes for urethral cancer and diseases that are clinically treated as urethral cancer. It contains both ICD-10 and SNOMED.', false, false, 'This valueset is multipurpose and is used in the ED Risk model, BI patient categorization and to identify patients for implementation purposes.', 'intensional');
INSERT INTO value_sets.value_set VALUES ('a757b6c0-635b-11ec-9eb7-612d55c4f5aa', 'Penile Cancer Combined', 'Penile Cancer Combined', 'Project Ronin', 'Hao Sun', 'This valueset contains the diagnosis codes for penile cancer and diseases that are clinically treated as penile cancer. It contains both ICD-10 and SNOMED.', false, false, 'This valueset is multipurpose and is used in the ED Risk model, BI patient categorization and to identify patients for implementation purposes.', 'intensional');
INSERT INTO value_sets.value_set VALUES ('8d2236a0-6d00-11ec-a5f0-91e137910fd5', 'Beta HCG', 'Beta HCG', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating Beta HCG measurement', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('a846c2c0-6d00-11ec-a5f0-91e137910fd5', 'BUN', 'BUN', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating BUNmeasurement', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('bd83c5c0-6d00-11ec-a5f0-91e137910fd5', 'CA125', 'CA125', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating CA125 measurement', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('de9224f0-6d00-11ec-a5f0-91e137910fd5', 'CA15.3', 'CA15.3', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating CA15.3 measurement', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('fb519c60-6d00-11ec-a5f0-91e137910fd5', 'CA19.9', 'CA19.9', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating CA19.9 measurement', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('6e885280-6d03-11ec-a5f0-91e137910fd5', 'CA72.4', 'CA72.4', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of Cancer Ag 72-4 [Units/volume] in Serum or Plasma ', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('873b6970-6d03-11ec-a5f0-91e137910fd5', 'Creatinine', 'Creatinine', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of Creatinine', false, false, 'To provide features for the ED risk mode', 'intensional');
INSERT INTO value_sets.value_set VALUES ('9ee044b0-6d03-11ec-a5f0-91e137910fd5', 'Direct bilirubin', 'Direct bilirubin', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of Direct bilirubin', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('b953d050-6d03-11ec-a5f0-91e137910fd5', 'Fibrinogen', 'Fibrinogen', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of Fibrinogen', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('0e27aa20-6d04-11ec-a5f0-91e137910fd5', 'HGB', 'HGB', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of HGB', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('26ce6500-6d04-11ec-a5f0-91e137910fd5', 'INR', 'INR', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of INR', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('f7f4ef70-6e56-11ec-843a-81f6989b647b', 'Pulmonary Circulation Disorder', 'Pulmonary Circulation Disorder', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for the condition of Pulmonary Circulation Disorder', false, false, 'Multipurpose; used in ED prediction model and patient cohorting ', 'intensional');
INSERT INTO value_sets.value_set VALUES ('43dc71a0-6d04-11ec-a5f0-91e137910fd5', 'LDH', 'LDH', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of LDH', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('5789ace0-6d04-11ec-a5f0-91e137910fd5', 'Lipase', 'Lipase', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of Lipase', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('68c39e30-6d04-11ec-a5f0-91e137910fd5', 'PLT count', 'PLT count', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of PLT count', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('7f346640-6d04-11ec-a5f0-91e137910fd5', 'Potassium', 'Potassium', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of Potassium', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('8c36a150-6d04-11ec-a5f0-91e137910fd5', 'PSA', 'PSA', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of PSA', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('9b8627c0-6d04-11ec-a5f0-91e137910fd5', 'PT', 'PT', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of PT', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('b0142610-6d04-11ec-a5f0-91e137910fd5', 'PTT', 'PTT', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of PTT', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('c466c910-6d04-11ec-a5f0-91e137910fd5', 'RBC', 'RBC', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of RBC', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('d6a07720-6d04-11ec-a5f0-91e137910fd5', 'Sodium', 'Sodium', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of Sodium', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('eb43f940-6d04-11ec-a5f0-91e137910fd5', 'Total bilirubin', 'Total bilirubin', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of Total bilirubin', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('faf045b0-6d04-11ec-a5f0-91e137910fd5', 'WBC', 'WBC', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of WBC', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('ec268680-6d03-11ec-a5f0-91e137910fd5', 'HCT', 'HCT', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of Hematocrit', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('2040eb90-6d8b-11ec-bcc4-f7e61651b088', 'Alcohol Abuse', 'Alcohol Abuse', 'Project Ronin', 'Katelin', 'SNOMED and ICD.10 for Alcohol Abuse', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('ffdb2310-6d77-11ec-b2ec-7d2ddcce13c5', 'Diarrhea', 'Diarrhea', 'Project Ronin', 'Katelin', 'SNOMED and ICD.10 from Diarrhea', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('48ea4050-6d77-11ec-9b8d-2bf05d332888', 'Nausea Vomiting', 'Nausea Vomiting', 'Project Ronin', 'Katelin', 'SNOMED and ICD.10 for nausea/vomiting', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('2c53fef0-6d7b-11ec-bea5-bd4d5e8164dd', 'Congestive Heart Failure', 'Congestive Heart Failure', 'Project Ronin', 'Katelin', 'SNOMED and ICD.10 for Congestive Heart Failure', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('b2bb62c0-6d7c-11ec-b1fa-71c6dd1818a3', 'Anemia', 'Anemia', 'Project Ronin', 'Katelin', 'SNOMED and ICD.10 for Anemia', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('53d297e0-6d7e-11ec-9e94-f35e8c0249ad', 'Neurological Disorders', 'Neurological Disorders', 'Project Ronin', 'Katelin', 'SNOMED and ICD.10 for Neurological Disorders', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('5f2287b0-6d81-11ec-bee7-e106dc640613', 'Rheumatoid Arthritis', 'Rheumatoid Arthritis', 'Project Ronin', 'Katelin', 'SNOMED and ICD.10 for Rheumatoid Arthritis', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('aadd9900-6daa-11ec-9f7d-ddc3ed69f759', 'Diabetes (Complicated)', 'Diabetes (Complicated)', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for diabetes (complicated)', false, false, 'This valueset is multipurpose and is used in the ED prediction model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('bb8afa60-6dad-11ec-aded-f11e9050236f', 'Other antineoplastic agents', 'Other antineoplastic agents', 'Project Ronin', 'Hao Sun', 'RxNorm codes for the ATC class of Other antineoplastic agents ', false, false, 'This valueset is multipurpose ', 'intensional');
INSERT INTO value_sets.value_set VALUES ('5ba2da00-6d85-11ec-b733-311671ebad29', 'Hypertension (Complicated)', 'Hypertension (Complicated)', 'Project Ronin', 'Katelin', 'SNOMED and ICD.10 for Hypertension (Complicated)', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('e9461a10-6d8b-11ec-92f6-4991e9e492ec', 'Hypertension', 'Hypertension', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for hypertension', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('341d30a0-6d8c-11ec-ac3e-fffa4d17d4a4', 'Drug Abuse', 'Drug Abuse', 'Project Ronin', 'Katelin', 'SNOMED and ICD.10 for Drug Abuse', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('64dde4d0-6d8e-11ec-a170-05e47613622b', 'Fluid-Electrolyte Disorder', 'Fluid-Electrolyte Disorder', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for fluid-electrolyte disorder ', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('2e3d5dc0-6d8e-11ec-b97f-b976d95b97d2', 'Peptic Ulcer Disease', 'Peptic Ulcer Disease', 'Project Ronin', 'Katelin', 'SNOMED and ICD.10 for Peptic Ulcer Disease', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('58f38cf0-6d8f-11ec-b520-41f15c77ac39', 'Blood Loss Anemia', 'Blood Loss Anemia', 'Project Ronin', 'Katelin', 'SNOMED and ICD10 codes for blood loss anemia', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('c4587e80-6d83-11ec-ae12-57a69b13a7b5', 'Valvular Disease', 'Valvular Disease', 'Project Ronin', 'Katelin', 'SNOMED and ICD.10 for Valvular Disease', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('53471430-6d93-11ec-9406-0d51e8580359', 'AST', 'AST', 'Project Ronin', 'Stephen Weaver', 'LOINC codes indicating the measurement of AST', false, false, 'To provide features for the ED risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('ce94fe50-6d8d-11ec-b97f-b976d95b97d2', 'Weight Loss', 'Weight Loss', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD.10 for weight loss', false, false, 'This valuset is multipurpose and is used in ED prediction model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('c2099cc0-6d94-11ec-9ba4-09f608633acd', 'Hypothyroidism', 'Hypothyroidism', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for hypothyroidism', false, false, 'This valueset is multipurpose and is used in the ED prediction model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('7e6baab0-6d96-11ec-bd52-17147d6e7f73', 'Psychoses', 'Psychoses', 'Project Ronin', 'Katelin', 'SNOMED and ICD10 codes for Psychoses', false, false, 'This valueset is multipurpose and is used in the ED prediction model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('458b5050-6d97-11ec-bbaf-a125da7f7984', 'Depression', 'Depression', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for depression', false, false, 'This valueset is multipurpose and is used in the ED prediction model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('bc157730-6d99-11ec-aa67-fdd64fa9f37f', 'Paralysis', 'Paralysis', 'Project Ronin', 'Katelin', 'SNOMED and ICD10 codes for paralysis', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('29a50560-6d98-11ec-afbe-617680d9103a', 'AIDS HIV', 'AIDS HIV', 'Project Ronin', 'Katelin ', 'SNOMED and ICD10 codes for AIDS/HIV', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('9ca0f4e0-6d78-11ec-8b39-799261a6657c', 'Fever', 'Fever', 'Project Ronin', 'Katelin', 'SNOMED and ICD.10 for fever', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('5b228770-6d7a-11ec-b87b-d7c29dbf0b8b', 'Pain', 'Pain', 'Project Ronin', 'Katelin', 'SNOMED and ICD.10 for pain', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('4e607f20-6d79-11ec-98b0-918f8949cc42', 'Shortness of Breath', 'Shortness of Breath', 'Project Ronin', 'kateliin', 'SNOMED and ICD.10 for Shortness of Breath', false, false, 'This valueset is multipurpose and is used in the ED Risk model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('e99cd250-6d9b-11ec-97fc-0f785516332e', 'Coagulopathy', 'Coagulopathy', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for coagulopathy', false, false, 'This valueset is multipurpose and is used in the ED prediction model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('48ac0990-6d9d-11ec-8a9d-0504338216ef', 'Liver Disease', 'Liver Disease', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for liver disease', false, false, 'This valueset is multipurpose and is used in the ED model prediction ', 'intensional');
INSERT INTO value_sets.value_set VALUES ('3f7cc3f0-6da7-11ec-bd70-69b80260bb41', 'Renal Failure', 'Renal Failure', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for renal failure', false, false, 'This valueset is multipurpose and is used in the ED prediction model', 'intensional');
INSERT INTO value_sets.value_set VALUES ('2af996e0-6dae-11ec-93e8-a5c70bfd1107', 'Plant alkaloids and other natural products', 'Plant alkaloids and other natural products', 'Project Ronin', 'Hao Sun', 'This valueset contains RxNorm codes for the ATC class of plant alkaloids and other natural products ', false, false, 'multipurpose; ED prediction model and patient cohorting ', 'intensional');
INSERT INTO value_sets.value_set VALUES ('94055fc0-6dae-11ec-b3fb-734142b5f479', 'Immunostimulants', 'Immunostimulants', 'Project Ronin', 'Hao Sun', 'This valueset contains RxNorm codes of members of the ATC class immunostimulants ', false, false, 'Multipurpose; ED prediction model and patient cohorting ', 'intensional');
INSERT INTO value_sets.value_set VALUES ('d9d05910-6dae-11ec-a1e9-35e80f62d80b', 'Immunosuppressants', 'Immunosuppressants', 'Project Ronin', 'Hao Sun', 'This valueset contains RxNorm codes for members of the ATC class immunosuppressants ', false, false, 'Multipurpose; ED prediction model and patient cohorting ', 'intensional');
INSERT INTO value_sets.value_set VALUES ('bf171510-6db2-11ec-bad0-8f90790692ae', 'Diabetes (Uncomplicated)', 'Diabetes (Uncomplicated)', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for diabetes (uncomplicated)', false, false, 'Multipurpose; ED prediction model and patient cohorting ', 'intensional');
INSERT INTO value_sets.value_set VALUES ('2e1ef920-6db6-11ec-95f4-fdc633495259', 'Cardiac Arrhythmia', 'Cardiac Arrhythmia', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for cardiac arrhythmia ', false, false, 'Multipurpose; ED prediction model and patient cohorting ', 'intensional');
INSERT INTO value_sets.value_set VALUES ('e2ea06f0-6dbc-11ec-bb58-8de119727b07', 'Endocrine Therapy', 'Endocrine Therapy', 'Project Ronin', 'Stephen Weaver', 'drugs indicating endocrine therapy', false, false, 'ED Risk Feature Generation', 'intensional');
INSERT INTO value_sets.value_set VALUES ('9309a820-6e52-11ec-9ea2-c7238c39a6c5', 'Obesity', 'Obesity', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for the condition obesity ', false, false, 'Multipurpose; ED model prediction and patient cohorting ', 'intensional');
INSERT INTO value_sets.value_set VALUES ('57d28c80-6e53-11ec-a288-9761f3821129', 'Peripheral Vascular Disorder', 'Peripheral Vascular Disorder', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for the condition of peripheral vascular disorder', false, false, 'Multipurpose; ED prediction model and patient cohorting ', 'intensional');
INSERT INTO value_sets.value_set VALUES ('b2fba890-6e57-11ec-8eff-a944250eaa2c', 'Chronic Pulmonary Disease', 'Chronic Pulmonary Disease', 'Project Ronin', 'Hao Sun', 'SNOMED and ICD10 codes for the condition of Chronic Pulmonary Disease', false, false, 'Multipurpose; ED prediction model and patient cohorting ', 'intensional');
INSERT INTO value_sets.value_set VALUES ('328b1fc4-27a8-11ec-9621-0242ac130002', 'extensional-test', 'Extensional Value Set Test', 'Project Ronin', 'Rey', 'Used for automated tests', false, true, 'testing', 'extensional');


-- Completed on 2022-01-11 15:58:39 MST

--
-- PostgreSQL database dump complete
--

