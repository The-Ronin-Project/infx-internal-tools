--
-- PostgreSQL database dump
--

-- Dumped from database version 11.12
-- Dumped by pg_dump version 14.1

-- Started on 2022-02-22 17:11:18 MST

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
-- TOC entry 28 (class 2615 OID 117263)
-- Name: loinc; Type: SCHEMA; Schema: -; Owner: -
--

-- CREATE SCHEMA loinc;


--
-- TOC entry 281 (class 1259 OID 117346)
-- Name: code; Type: TABLE; Schema: loinc; Owner: -
--

CREATE TABLE loinc.code (
    loinc_num character varying(10) NOT NULL,
    component character varying(255),
    property character varying(255),
    time_aspct character varying(255),
    system character varying(255),
    scale_typ character varying(255),
    method_typ character varying(255),
    class character varying(255),
    versionlastchanged character varying(255),
    chng_type character varying(255),
    definitiondescription text,
    status character varying(255),
    consumer_name character varying(255),
    classtype integer,
    formula text,
    exmpl_answers text,
    survey_quest_text text,
    survey_quest_src character varying(50),
    unitsrequired character varying(1),
    submitted_units character varying(30),
    relatednames2 text,
    shortname character varying(255),
    order_obs character varying(15),
    cdisc_common_tests character varying(1),
    hl7_field_subfield_id character varying(50),
    external_copyright_notice text,
    example_units character varying(255),
    long_common_name character varying(255),
    unitsandrange text,
    example_ucum_units character varying(255),
    example_si_ucum_units character varying(255),
    status_reason character varying(9),
    status_text text,
    change_reason_public text,
    common_test_rank integer,
    common_order_rank integer,
    common_si_test_rank integer,
    hl7_attachment_structure character varying(15),
    externalcopyrightlink character varying(255),
    paneltype character varying(50),
    askatorderentry character varying(255),
    associatedobservations character varying(255),
    versionfirstreleased character varying(255),
    validhl7attachmentrequest character varying(50),
    displayname character varying(255),
    terminology_version_uuid uuid
);


--
-- TOC entry 293 (class 1259 OID 119438)
-- Name: component; Type: VIEW; Schema: loinc; Owner: -
--

-- CREATE VIEW loinc.component AS
--  SELECT DISTINCT code.component
--    FROM loinc.code;


--
-- TOC entry 282 (class 1259 OID 117354)
-- Name: mapto; Type: TABLE; Schema: loinc; Owner: -
--

CREATE TABLE loinc.mapto (
    loinc character varying(10) NOT NULL,
    map_to character varying(10) NOT NULL,
    comment text,
    terminology_version_uuid uuid
);


--
-- TOC entry 280 (class 1259 OID 117338)
-- Name: sourceorganization; Type: TABLE; Schema: loinc; Owner: -
--

CREATE TABLE loinc.sourceorganization (
    id integer,
    copyright_id character varying(255) NOT NULL,
    name character varying(255),
    copyright text,
    terms_of_use text,
    url text,
    terminology_version_uuid uuid
);


--
-- TOC entry 5623 (class 2606 OID 164758)
-- Name: code code_version_unique; Type: CONSTRAINT; Schema: loinc; Owner: -
--

-- ALTER TABLE ONLY loinc.code
--     ADD CONSTRAINT code_version_unique UNIQUE (loinc_num, terminology_version_uuid) INCLUDE (loinc_num, terminology_version_uuid);


--
-- TOC entry 5625 (class 2606 OID 165218)
-- Name: mapto code_version_unique_mapto; Type: CONSTRAINT; Schema: loinc; Owner: -
--

-- ALTER TABLE ONLY loinc.mapto
--     ADD CONSTRAINT code_version_unique_mapto UNIQUE (loinc, map_to, terminology_version_uuid) INCLUDE (loinc, map_to, terminology_version_uuid);


--
-- TOC entry 5621 (class 2606 OID 165220)
-- Name: sourceorganization copyright_version_unique; Type: CONSTRAINT; Schema: loinc; Owner: -
--

-- ALTER TABLE ONLY loinc.sourceorganization
--     ADD CONSTRAINT copyright_version_unique UNIQUE (copyright_id, terminology_version_uuid) INCLUDE (copyright_id, terminology_version_uuid);


--
-- TOC entry 5626 (class 2606 OID 164643)
-- Name: sourceorganization terminology_version_uuid_fk; Type: FK CONSTRAINT; Schema: loinc; Owner: -
--

-- ALTER TABLE ONLY loinc.sourceorganization
--     ADD CONSTRAINT terminology_version_uuid_fk FOREIGN KEY (terminology_version_uuid) REFERENCES public.terminology_versions(uuid) NOT VALID;


--
-- TOC entry 5628 (class 2606 OID 164648)
-- Name: mapto terminology_version_uuid_fk; Type: FK CONSTRAINT; Schema: loinc; Owner: -
--

-- ALTER TABLE ONLY loinc.mapto
--     ADD CONSTRAINT terminology_version_uuid_fk FOREIGN KEY (terminology_version_uuid) REFERENCES public.terminology_versions(uuid) NOT VALID;


--
-- TOC entry 5627 (class 2606 OID 164653)
-- Name: code terminology_version_uuid_fk; Type: FK CONSTRAINT; Schema: loinc; Owner: -
--

-- ALTER TABLE ONLY loinc.code
--     ADD CONSTRAINT terminology_version_uuid_fk FOREIGN KEY (terminology_version_uuid) REFERENCES public.terminology_versions(uuid) NOT VALID;


-- Completed on 2022-02-22 17:11:22 MST

--
-- PostgreSQL database dump complete
--

