--
-- PostgreSQL database dump
--

-- Dumped from database version 11.11
-- Dumped by pg_dump version 13.3

-- Started on 2022-01-24 13:12:53 MST

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
-- TOC entry 16 (class 2615 OID 49638)
-- Name: organizations; Type: SCHEMA; Schema: -; Owner: -
--

-- CREATE SCHEMA organizations;


--
-- TOC entry 240 (class 1259 OID 49647)
-- Name: hierarchy; Type: TABLE; Schema: organizations; Owner: -
--

CREATE TABLE organizations.hierarchy (
    relationship_uuid uuid NOT NULL,
    source_organization_uuid uuid NOT NULL,
    relationship_type character varying NOT NULL,
    target_organization_uuid uuid NOT NULL
);


--
-- TOC entry 239 (class 1259 OID 49639)
-- Name: organizations; Type: TABLE; Schema: organizations; Owner: -
--

CREATE TABLE organizations.organizations (
    uuid uuid NOT NULL,
    name character varying NOT NULL
);


--
-- TOC entry 4556 (class 0 OID 49647)
-- Dependencies: 240
-- Data for Name: hierarchy; Type: TABLE DATA; Schema: organizations; Owner: -
--

INSERT INTO organizations.hierarchy VALUES ('858d1100-ff85-11eb-9f47-ffa6d132f8a4', '2467c7d0-ff80-11eb-b809-7f2b9aa56ab4', 'Has Child', 'ef8fd980-ff84-11eb-9f47-ffa6d132f8a4');
INSERT INTO organizations.hierarchy VALUES ('8c695a60-ff85-11eb-9f47-ffa6d132f8a4', '2467c7d0-ff80-11eb-b809-7f2b9aa56ab4', 'Has Child', '866632f0-ff85-11eb-9f47-ffa6d132f8a4');
INSERT INTO organizations.hierarchy VALUES ('ac6ac500-0107-11ec-bb43-f710353c022d', '90297e40-0107-11ec-bb43-f710353c022d', 'Has Child', '9e512680-0107-11ec-bb43-f710353c022d');
INSERT INTO organizations.hierarchy VALUES ('b2125ef0-0107-11ec-bb43-f710353c022d', '90297e40-0107-11ec-bb43-f710353c022d', 'Has Child', 'ad3edde0-0107-11ec-bb43-f710353c022d');
INSERT INTO organizations.hierarchy VALUES ('dbd5fdd0-41a0-11ec-a6cf-fbe859f8d201', 'a82c6fa0-41a0-11ec-a6cf-fbe859f8d201', 'Has Child', 'd61ed380-41a0-11ec-a6cf-fbe859f8d201');
INSERT INTO organizations.hierarchy VALUES ('139b3c70-426a-11ec-bd76-415fa1681ecf', 'a82c6fa0-41a0-11ec-a6cf-fbe859f8d201', 'Has Child', 'fdf58c90-4269-11ec-bd76-415fa1681ecf');
INSERT INTO organizations.hierarchy VALUES ('222c54e0-426a-11ec-bd76-415fa1681ecf', 'a82c6fa0-41a0-11ec-a6cf-fbe859f8d201', 'Has Child', '13fd0b80-426a-11ec-bd76-415fa1681ecf');


--
-- TOC entry 4555 (class 0 OID 49639)
-- Dependencies: 239
-- Data for Name: organizations; Type: TABLE DATA; Schema: organizations; Owner: -
--

INSERT INTO organizations.organizations VALUES ('2467c7d0-ff80-11eb-b809-7f2b9aa56ab4', 'MD Anderson');
INSERT INTO organizations.organizations VALUES ('ef8fd980-ff84-11eb-9f47-ffa6d132f8a4', 'Breast Medical Oncology');
INSERT INTO organizations.organizations VALUES ('866632f0-ff85-11eb-9f47-ffa6d132f8a4', 'Genitourinary Medical Oncology');
INSERT INTO organizations.organizations VALUES ('90297e40-0107-11ec-bb43-f710353c022d', 'Project Ronin');
INSERT INTO organizations.organizations VALUES ('9e512680-0107-11ec-bb43-f710353c022d', 'Test Breast Organization');
INSERT INTO organizations.organizations VALUES ('ad3edde0-0107-11ec-bb43-f710353c022d', 'Test GU Organization');
INSERT INTO organizations.organizations VALUES ('a82c6fa0-41a0-11ec-a6cf-fbe859f8d201', 'Providence Saint John''s');
INSERT INTO organizations.organizations VALUES ('d61ed380-41a0-11ec-a6cf-fbe859f8d201', 'Genitourinary Dept');
INSERT INTO organizations.organizations VALUES ('fdf58c90-4269-11ec-bd76-415fa1681ecf', 'Breast ');
INSERT INTO organizations.organizations VALUES ('13fd0b80-426a-11ec-bd76-415fa1681ecf', 'Neuro-oncology');


-- Completed on 2022-01-24 13:13:01 MST

--
-- PostgreSQL database dump complete
--

