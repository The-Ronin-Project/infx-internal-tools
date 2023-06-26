--
-- PostgreSQL database dump
--

-- Dumped from database version 11.11
-- Dumped by pg_dump version 13.3

-- Started on 2022-01-11 16:02:13 MST

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
-- TOC entry 272 (class 1259 OID 89536)
-- Name: extensional_member; Type: TABLE; Schema: value_sets; Owner: -
--

CREATE TABLE value_sets.extensional_member (
    uuid uuid NOT NULL,
    code character varying NOT NULL,
    added_by character varying NOT NULL,
    vs_version_uuid uuid NOT NULL,
    terminology_version_uuid uuid NOT NULL,
    display character varying NOT NULL
);


--
-- TOC entry 4550 (class 0 OID 89536)
-- Dependencies: 272
-- Data for Name: extensional_member; Type: TABLE DATA; Schema: value_sets; Owner: -
--

INSERT INTO value_sets.extensional_member VALUES ('157068b8-27a8-11ec-9621-0242ac130002', '444604002', 'Rey Johnson', '987ffe8a-27a8-11ec-9621-0242ac130002', '3b07a086-2227-11ec-9621-0242ac130002', 'Carcinoma of breast with ductal and lobular features (disorder)');
INSERT INTO value_sets.extensional_member VALUES ('1f5e6b6c-27a9-11ec-9621-0242ac130002', '372092003', 'Rey Johnson', '987ffe8a-27a8-11ec-9621-0242ac130002', '3b07a086-2227-11ec-9621-0242ac130002', 'Primary malignant neoplasm of axillary tail of breast (disorder)');


-- Completed on 2022-01-11 16:02:18 MST

--
-- PostgreSQL database dump complete
--

