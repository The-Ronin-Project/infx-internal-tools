--
-- PostgreSQL database dump
--

-- Dumped from database version 11.11
-- Dumped by pg_dump version 13.3

-- Started on 2021-10-19 10:48:35 MDT

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
-- TOC entry 20 (class 2615 OID 32711)
-- Name: snomedct; Type: SCHEMA; Schema: -; Owner: -
--

-- CREATE SCHEMA snomedct;


--
-- TOC entry 232 (class 1259 OID 32748)
-- Name: associationrefset_f; Type: TABLE; Schema: snomedct; Owner: -
--

CREATE TABLE snomedct.associationrefset_f (
    id uuid NOT NULL,
    effectivetime character(8) NOT NULL,
    active character(1) NOT NULL,
    moduleid character varying(18) NOT NULL,
    refsetid character varying(18) NOT NULL,
    referencedcomponentid character varying(18) NOT NULL,
    targetcomponentid character varying(18) NOT NULL
);


--
-- TOC entry 233 (class 1259 OID 32753)
-- Name: attributevaluerefset_f; Type: TABLE; Schema: snomedct; Owner: -
--

CREATE TABLE snomedct.attributevaluerefset_f (
    id uuid NOT NULL,
    effectivetime character(8) NOT NULL,
    active character(1) NOT NULL,
    moduleid character varying(18) NOT NULL,
    refsetid character varying(18) NOT NULL,
    referencedcomponentid character varying(18) NOT NULL,
    valueid character varying(18) NOT NULL
);


--
-- TOC entry 236 (class 1259 OID 32771)
-- Name: complexmaprefset_f; Type: TABLE; Schema: snomedct; Owner: -
--

CREATE TABLE snomedct.complexmaprefset_f (
    id uuid NOT NULL,
    effectivetime character(8) NOT NULL,
    active character(1) NOT NULL,
    moduleid character varying(18) NOT NULL,
    refsetid character varying(18) NOT NULL,
    referencedcomponentid character varying(18) NOT NULL,
    mapgroup smallint NOT NULL,
    mappriority smallint NOT NULL,
    maprule text,
    mapadvice text,
    maptarget text,
    correlationid character varying(18) NOT NULL
);


--
-- TOC entry 226 (class 1259 OID 32712)
-- Name: concept_f; Type: TABLE; Schema: snomedct; Owner: -
--

CREATE TABLE snomedct.concept_f (
    id character varying(18) NOT NULL,
    effectivetime character(8) NOT NULL,
    active character(1) NOT NULL,
    moduleid character varying(18) NOT NULL,
    definitionstatusid character varying(18) NOT NULL
);


--
-- TOC entry 227 (class 1259 OID 32717)
-- Name: description_f; Type: TABLE; Schema: snomedct; Owner: -
--

CREATE TABLE snomedct.description_f (
    id character varying(18) NOT NULL,
    effectivetime character(8) NOT NULL,
    active character(1) NOT NULL,
    moduleid character varying(18) NOT NULL,
    conceptid character varying(18) NOT NULL,
    languagecode character varying(2) NOT NULL,
    typeid character varying(18) NOT NULL,
    term text NOT NULL,
    casesignificanceid character varying(18) NOT NULL
);


--
-- TOC entry 237 (class 1259 OID 32779)
-- Name: extendedmaprefset_f; Type: TABLE; Schema: snomedct; Owner: -
--

CREATE TABLE snomedct.extendedmaprefset_f (
    id uuid NOT NULL,
    effectivetime character(8) NOT NULL,
    active character(1) NOT NULL,
    moduleid character varying(18) NOT NULL,
    refsetid character varying(18) NOT NULL,
    referencedcomponentid character varying(18) NOT NULL,
    mapgroup smallint NOT NULL,
    mappriority smallint NOT NULL,
    maprule text,
    mapadvice text,
    maptarget text,
    correlationid character varying(18),
    mapcategoryid character varying(18)
);


--
-- TOC entry 231 (class 1259 OID 32743)
-- Name: langrefset_f; Type: TABLE; Schema: snomedct; Owner: -
--

CREATE TABLE snomedct.langrefset_f (
    id uuid NOT NULL,
    effectivetime character(8) NOT NULL,
    active character(1) NOT NULL,
    moduleid character varying(18) NOT NULL,
    refsetid character varying(18) NOT NULL,
    referencedcomponentid character varying(18) NOT NULL,
    acceptabilityid character varying(18) NOT NULL
);


--
-- TOC entry 229 (class 1259 OID 32733)
-- Name: relationship_f; Type: TABLE; Schema: snomedct; Owner: -
--

CREATE TABLE snomedct.relationship_f (
    id character varying(18) NOT NULL,
    effectivetime character(8) NOT NULL,
    active character(1) NOT NULL,
    moduleid character varying(18) NOT NULL,
    sourceid character varying(18) NOT NULL,
    destinationid character varying(18) NOT NULL,
    relationshipgroup character varying(18) NOT NULL,
    typeid character varying(18) NOT NULL,
    characteristictypeid character varying(18) NOT NULL,
    modifierid character varying(18) NOT NULL
);


--
-- TOC entry 235 (class 1259 OID 32763)
-- Name: simplemaprefset_f; Type: TABLE; Schema: snomedct; Owner: -
--

CREATE TABLE snomedct.simplemaprefset_f (
    id uuid NOT NULL,
    effectivetime character(8) NOT NULL,
    active character(1) NOT NULL,
    moduleid character varying(18) NOT NULL,
    refsetid character varying(18) NOT NULL,
    referencedcomponentid character varying(18) NOT NULL,
    maptarget text NOT NULL
);


--
-- TOC entry 234 (class 1259 OID 32758)
-- Name: simplerefset_f; Type: TABLE; Schema: snomedct; Owner: -
--

CREATE TABLE snomedct.simplerefset_f (
    id uuid NOT NULL,
    effectivetime character(8) NOT NULL,
    active character(1) NOT NULL,
    moduleid character varying(18) NOT NULL,
    refsetid character varying(18) NOT NULL,
    referencedcomponentid character varying(18) NOT NULL
);


--
-- TOC entry 230 (class 1259 OID 32738)
-- Name: stated_relationship_f; Type: TABLE; Schema: snomedct; Owner: -
--

CREATE TABLE snomedct.stated_relationship_f (
    id character varying(18) NOT NULL,
    effectivetime character(8) NOT NULL,
    active character(1) NOT NULL,
    moduleid character varying(18) NOT NULL,
    sourceid character varying(18) NOT NULL,
    destinationid character varying(18) NOT NULL,
    relationshipgroup character varying(18) NOT NULL,
    typeid character varying(18) NOT NULL,
    characteristictypeid character varying(18) NOT NULL,
    modifierid character varying(18) NOT NULL
);


--
-- TOC entry 228 (class 1259 OID 32725)
-- Name: textdefinition_f; Type: TABLE; Schema: snomedct; Owner: -
--

CREATE TABLE snomedct.textdefinition_f (
    id character varying(18) NOT NULL,
    effectivetime character(8) NOT NULL,
    active character(1) NOT NULL,
    moduleid character varying(18) NOT NULL,
    conceptid character varying(18) NOT NULL,
    languagecode character varying(2) NOT NULL,
    typeid character varying(18) NOT NULL,
    term text NOT NULL,
    casesignificanceid character varying(18) NOT NULL
);


--
-- TOC entry 4397 (class 2606 OID 32752)
-- Name: associationrefset_f associationrefset_f_pkey; Type: CONSTRAINT; Schema: snomedct; Owner: -
--

-- ALTER TABLE ONLY snomedct.associationrefset_f
--     ADD CONSTRAINT associationrefset_f_pkey PRIMARY KEY (id, effectivetime);


--
-- TOC entry 4400 (class 2606 OID 32757)
-- Name: attributevaluerefset_f attributevaluerefset_f_pkey; Type: CONSTRAINT; Schema: snomedct; Owner: -
--

-- ALTER TABLE ONLY snomedct.attributevaluerefset_f
--     ADD CONSTRAINT attributevaluerefset_f_pkey PRIMARY KEY (id, effectivetime);


-- --
-- -- TOC entry 4407 (class 2606 OID 32778)
-- -- Name: complexmaprefset_f complexmaprefset_f_pkey; Type: CONSTRAINT; Schema: snomedct; Owner: -
-- --

-- ALTER TABLE ONLY snomedct.complexmaprefset_f
--     ADD CONSTRAINT complexmaprefset_f_pkey PRIMARY KEY (id, effectivetime);


-- --
-- -- TOC entry 4379 (class 2606 OID 32716)
-- -- Name: concept_f concept_f_pkey; Type: CONSTRAINT; Schema: snomedct; Owner: -
-- --

-- ALTER TABLE ONLY snomedct.concept_f
--     ADD CONSTRAINT concept_f_pkey PRIMARY KEY (id, effectivetime);


-- --
-- -- TOC entry 4382 (class 2606 OID 32724)
-- -- Name: description_f description_f_pkey; Type: CONSTRAINT; Schema: snomedct; Owner: -
-- --

-- ALTER TABLE ONLY snomedct.description_f
--     ADD CONSTRAINT description_f_pkey PRIMARY KEY (id, effectivetime);


-- --
-- -- TOC entry 4410 (class 2606 OID 32786)
-- -- Name: extendedmaprefset_f extendedmaprefset_f_pkey; Type: CONSTRAINT; Schema: snomedct; Owner: -
-- --

-- ALTER TABLE ONLY snomedct.extendedmaprefset_f
--     ADD CONSTRAINT extendedmaprefset_f_pkey PRIMARY KEY (id, effectivetime);


-- --
-- -- TOC entry 4393 (class 2606 OID 32747)
-- -- Name: langrefset_f langrefset_f_pkey; Type: CONSTRAINT; Schema: snomedct; Owner: -
-- --

-- ALTER TABLE ONLY snomedct.langrefset_f
--     ADD CONSTRAINT langrefset_f_pkey PRIMARY KEY (id, effectivetime);


-- --
-- -- TOC entry 4388 (class 2606 OID 32737)
-- -- Name: relationship_f relationship_f_pkey; Type: CONSTRAINT; Schema: snomedct; Owner: -
-- --

-- ALTER TABLE ONLY snomedct.relationship_f
--     ADD CONSTRAINT relationship_f_pkey PRIMARY KEY (id, effectivetime);


-- --
-- -- TOC entry 4405 (class 2606 OID 32770)
-- -- Name: simplemaprefset_f simplemaprefset_f_pkey; Type: CONSTRAINT; Schema: snomedct; Owner: -
-- --

-- ALTER TABLE ONLY snomedct.simplemaprefset_f
--     ADD CONSTRAINT simplemaprefset_f_pkey PRIMARY KEY (id, effectivetime);


-- --
-- -- TOC entry 4402 (class 2606 OID 32762)
-- -- Name: simplerefset_f simplerefset_f_pkey; Type: CONSTRAINT; Schema: snomedct; Owner: -
-- --

-- ALTER TABLE ONLY snomedct.simplerefset_f
--     ADD CONSTRAINT simplerefset_f_pkey PRIMARY KEY (id, effectivetime);


-- --
-- -- TOC entry 4391 (class 2606 OID 32742)
-- -- Name: stated_relationship_f stated_relationship_f_pkey; Type: CONSTRAINT; Schema: snomedct; Owner: -
-- --

-- ALTER TABLE ONLY snomedct.stated_relationship_f
--     ADD CONSTRAINT stated_relationship_f_pkey PRIMARY KEY (id, effectivetime);


-- --
-- -- TOC entry 4385 (class 2606 OID 32732)
-- -- Name: textdefinition_f textdefinition_f_pkey; Type: CONSTRAINT; Schema: snomedct; Owner: -
-- --

-- ALTER TABLE ONLY snomedct.textdefinition_f
--     ADD CONSTRAINT textdefinition_f_pkey PRIMARY KEY (id, effectivetime);


-- --
-- -- TOC entry 4395 (class 1259 OID 32792)
-- -- Name: associationrefset_f_idx; Type: INDEX; Schema: snomedct; Owner: -
-- --

-- CREATE INDEX associationrefset_f_idx ON snomedct.associationrefset_f USING btree (referencedcomponentid, targetcomponentid);


-- --
-- -- TOC entry 4398 (class 1259 OID 32793)
-- -- Name: attributevaluerefset_f_idx; Type: INDEX; Schema: snomedct; Owner: -
-- --

-- CREATE INDEX attributevaluerefset_f_idx ON snomedct.attributevaluerefset_f USING btree (referencedcomponentid, valueid);


-- --
-- -- TOC entry 4408 (class 1259 OID 32795)
-- -- Name: complexmaprefset_referencedcomponentid_idx; Type: INDEX; Schema: snomedct; Owner: -
-- --

-- CREATE INDEX complexmaprefset_referencedcomponentid_idx ON snomedct.complexmaprefset_f USING btree (referencedcomponentid);


-- --
-- -- TOC entry 4380 (class 1259 OID 32787)
-- -- Name: description_conceptid_idx; Type: INDEX; Schema: snomedct; Owner: -
-- --

-- CREATE INDEX description_conceptid_idx ON snomedct.description_f USING btree (conceptid);


-- --
-- -- TOC entry 4411 (class 1259 OID 32796)
-- -- Name: extendedmaprefset_referencedcomponentid_idx; Type: INDEX; Schema: snomedct; Owner: -
-- --

-- CREATE INDEX extendedmaprefset_referencedcomponentid_idx ON snomedct.extendedmaprefset_f USING btree (referencedcomponentid);


-- --
-- -- TOC entry 4394 (class 1259 OID 32791)
-- -- Name: langrefset_referencedcomponentid_idx; Type: INDEX; Schema: snomedct; Owner: -
-- --

-- CREATE INDEX langrefset_referencedcomponentid_idx ON snomedct.langrefset_f USING btree (referencedcomponentid);


-- --
-- -- TOC entry 4386 (class 1259 OID 32789)
-- -- Name: relationship_f_idx; Type: INDEX; Schema: snomedct; Owner: -
-- --

-- CREATE INDEX relationship_f_idx ON snomedct.relationship_f USING btree (sourceid, destinationid);


-- --
-- -- TOC entry 4403 (class 1259 OID 32794)
-- -- Name: simplerefset_referencedcomponentid_idx; Type: INDEX; Schema: snomedct; Owner: -
-- --

-- CREATE INDEX simplerefset_referencedcomponentid_idx ON snomedct.simplerefset_f USING btree (referencedcomponentid);


-- --
-- -- TOC entry 4389 (class 1259 OID 32790)
-- -- Name: stated_relationship_f_idx; Type: INDEX; Schema: snomedct; Owner: -
-- --

-- CREATE INDEX stated_relationship_f_idx ON snomedct.stated_relationship_f USING btree (sourceid, destinationid);


-- --
-- -- TOC entry 4383 (class 1259 OID 32788)
-- -- Name: textdefinition_conceptid_idx; Type: INDEX; Schema: snomedct; Owner: -
-- --

-- CREATE INDEX textdefinition_conceptid_idx ON snomedct.textdefinition_f USING btree (conceptid);


-- Completed on 2021-10-19 10:48:41 MDT

--
-- PostgreSQL database dump complete
--

