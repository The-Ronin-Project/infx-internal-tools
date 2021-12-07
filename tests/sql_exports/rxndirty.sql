--
-- PostgreSQL database dump
--

-- Dumped from database version 11.11
-- Dumped by pg_dump version 13.3

-- Started on 2021-10-19 14:10:04 MDT

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
-- TOC entry 21 (class 2615 OID 74902)
-- Name: rxnormDirty; Type: SCHEMA; Schema: -; Owner: -
--

-- CREATE SCHEMA "rxnormDirty";


--
-- TOC entry 257 (class 1259 OID 75702)
-- Name: rxnatomarchive; Type: TABLE; Schema: rxnormDirty; Owner: -
--

CREATE TABLE "rxnormDirty".rxnatomarchive (
    rxaui character varying(8) NOT NULL,
    aui character varying(10),
    str character varying(4000) NOT NULL,
    archive_timestamp character varying(280) NOT NULL,
    created_timestamp character varying(280) NOT NULL,
    updated_timestamp character varying(280) NOT NULL,
    code character varying(50),
    is_brand character varying(1),
    lat character varying(3),
    last_released character varying(30),
    saui character varying(50),
    vsab character varying(40),
    rxcui character varying(8),
    sab character varying(20),
    tty character varying(20),
    merged_to_rxcui character varying(8)
);


--
-- TOC entry 265 (class 1259 OID 75752)
-- Name: rxnconso; Type: TABLE; Schema: rxnormDirty; Owner: -
--

CREATE TABLE "rxnormDirty".rxnconso (
    rxcui character varying(10) NOT NULL,
    lat character varying(10) NOT NULL,
    ts character varying(10),
    lui character varying(10),
    stt character varying(10),
    sui character varying(10),
    ispref character varying(10),
    rxaui character varying(10) NOT NULL,
    saui character varying(50),
    scui character varying(50),
    sdui character varying(50),
    sab character varying(100),
    tty character varying(20),
    code character varying(100),
    str character varying(3000),
    srl character varying(50),
    suppress character varying(1000),
    cvf character varying(50)
);


--
-- TOC entry 264 (class 1259 OID 75748)
-- Name: rxncui; Type: TABLE; Schema: rxnormDirty; Owner: -
--

CREATE TABLE "rxnormDirty".rxncui (
    cui1 character varying(10),
    ver_start character varying(40),
    ver_end character varying(40),
    cardinality character varying(10),
    cui2 character varying(10)
);


--
-- TOC entry 263 (class 1259 OID 75742)
-- Name: rxncuichanges; Type: TABLE; Schema: rxnormDirty; Owner: -
--

CREATE TABLE "rxnormDirty".rxncuichanges (
    rxaui character varying(8),
    code character varying(50),
    sab character varying(20),
    tty character varying(20),
    str character varying(3000),
    old_rxcui character varying(8) NOT NULL,
    new_rxcui character varying(8) NOT NULL
);


--
-- TOC entry 256 (class 1259 OID 75658)
-- Name: rxndoc; Type: TABLE; Schema: rxnormDirty; Owner: -
--

CREATE TABLE "rxnormDirty".rxndoc (
    "KEY" character varying(50),
    "VALUE" character varying(1000),
    "TYPE" character varying(50),
    "EXPL" character varying(1000)
);


--
-- TOC entry 258 (class 1259 OID 75715)
-- Name: rxnrel; Type: TABLE; Schema: rxnormDirty; Owner: -
--

CREATE TABLE "rxnormDirty".rxnrel (
    rxcui1 character varying(10),
    rxaui1 character varying(10),
    stype1 character varying(50),
    rel character varying(10),
    rxcui2 character varying(10),
    rxaui2 character varying(10),
    stype2 character varying(50),
    rela character varying(100),
    rui character varying(10),
    srui character varying(50),
    sab character varying(100) NOT NULL,
    sl character varying(1000),
    dir character varying(10),
    rg character varying(10),
    suppress character varying(1000),
    cvf character varying(50)
);


--
-- TOC entry 259 (class 1259 OID 75721)
-- Name: rxnsab; Type: TABLE; Schema: rxnormDirty; Owner: -
--

CREATE TABLE "rxnormDirty".rxnsab (
    vcui character varying(10),
    rcui character varying(10),
    vsab character varying(40),
    rsab character varying(20) NOT NULL,
    son character varying(3000),
    sf character varying(20),
    sver character varying(20),
    vstart character varying(10),
    vend character varying(10),
    imeta character varying(10),
    rmeta character varying(10),
    slc character varying(1000),
    scc character varying(1000),
    srl integer,
    tfr integer,
    cfr integer,
    cxty character varying(50),
    ttyl character varying(300),
    atnl character varying(1000),
    lat character varying(10),
    cenc character varying(20),
    curver character varying(10),
    sabin character varying(10),
    ssn character varying(3000),
    scit character varying(4000)
);


--
-- TOC entry 260 (class 1259 OID 75727)
-- Name: rxnsat; Type: TABLE; Schema: rxnormDirty; Owner: -
--

CREATE TABLE "rxnormDirty".rxnsat (
    rxcui character varying(10),
    lui character varying(10),
    sui character varying(10),
    rxaui character varying(10),
    stype character varying(50),
    code character varying(50),
    atui character varying(11),
    satui character varying(50),
    atn character varying(1000) NOT NULL,
    sab character varying(100) NOT NULL,
    atv character varying(4000),
    suppress character varying(1000),
    cvf character varying(50)
);


--
-- TOC entry 261 (class 1259 OID 75733)
-- Name: rxnsty; Type: TABLE; Schema: rxnormDirty; Owner: -
--

CREATE TABLE "rxnormDirty".rxnsty (
    rxcui character varying(10) NOT NULL,
    tui character varying(10),
    stn character varying(100),
    sty character varying(50),
    atui character varying(11),
    cvf character varying(50)
);


-- Completed on 2021-10-19 14:10:09 MDT

--
-- PostgreSQL database dump complete
--

