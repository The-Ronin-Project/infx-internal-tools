--
-- PostgreSQL database dump
--

-- Dumped from database version 11.12
-- Dumped by pg_dump version 14.1

-- Started on 2022-02-17 12:47:21 MST

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
-- TOC entry 616 (class 1259 OID 164565)
-- Name: resource_synonyms; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE resource_synonyms (
    uuid uuid NOT NULL,
    resource_uuid uuid NOT NULL,
    context character varying NOT NULL,
    synonym character varying NOT NULL
);


--
-- TOC entry 5742 (class 0 OID 164565)
-- Dependencies: 616
-- Data for Name: resource_synonyms; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO resource_synonyms (uuid, resource_uuid, context, synonym) VALUES ('a7d609c2-12c2-4b06-b194-60dfa0ccebc2', 'e1ba9050-4c8e-11ec-99d0-6d735801ea9a', 'BI', 'Bladder Cancer');
INSERT INTO resource_synonyms (uuid, resource_uuid, context, synonym) VALUES ('ffa04a40-9024-11ec-8878-abb765df1f5b', '51801230-52ec-11ec-89f4-eb5726153df6', 'BI', 'Kidney Cancer');
INSERT INTO resource_synonyms (uuid, resource_uuid, context, synonym) VALUES ('16fb2430-9025-11ec-8878-abb765df1f5b', 'a757b6c0-635b-11ec-9eb7-612d55c4f5aa', 'BI', 'Penile Cancer');
INSERT INTO resource_synonyms (uuid, resource_uuid, context, synonym) VALUES ('e1b7c9e0-9029-11ec-b7ee-79641d026db3', 'bfcb8eb0-6343-11ec-bd13-cbbf4db9fbeb', 'TEST', 'Breast Cancer');


-- Completed on 2022-02-17 12:47:25 MST

--
-- PostgreSQL database dump complete
--

