--
-- PostgreSQL database dump
--

-- Dumped from database version 11.11
-- Dumped by pg_dump version 13.3

-- Started on 2022-01-11 16:05:12 MST

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
-- TOC entry 245 (class 1259 OID 56716)
-- Name: value_set_version; Type: TABLE; Schema: value_sets; Owner: -
--

CREATE TABLE value_sets.value_set_version (
    uuid uuid NOT NULL,
    effective_start date,
    effective_end date,
    value_set_uuid uuid,
    status character varying,
    description character varying,
    created_date date,
    version integer NOT NULL,
    comments text
);


--
-- TOC entry 4553 (class 0 OID 56716)
-- Dependencies: 245
-- Data for Name: value_set_version; Type: TABLE DATA; Schema: value_sets; Owner: -
--

INSERT INTO value_sets.value_set_version VALUES ('57f28490-6d77-11ec-8b47-03576f32531f', '2022-01-01', '2023-01-01', '48ea4050-6d77-11ec-9b8d-2bf05d332888', 'active', '', '2022-01-04', 1, 'Status updated to active by Stephen Weaver on January 7th 2022, 12:30:03 pm');
INSERT INTO value_sets.value_set_version VALUES ('64c5d2c2-2857-11ec-9621-0242ac130002', '2021-01-01', '2021-12-31', '245ec81a-2857-11ec-9621-0242ac130002', 'draft', 'initial version', '2021-11-09', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('4190b3d0-6d7b-11ec-b446-d58ab9e12bbb', '2022-01-01', '2023-01-01', '2c53fef0-6d7b-11ec-bea5-bd4d5e8164dd', 'active', '', '2022-01-04', 1, 'Status updated to active by Hao Sun on January 7th 2022, 11:34:22 am');
INSERT INTO value_sets.value_set_version VALUES ('4a4ef2e0-6d97-11ec-a703-b98eab3d2692', '2022-01-04', '2023-01-04', '458b5050-6d97-11ec-bbaf-a125da7f7984', 'active', '', '2022-01-04', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 2:39:03 pm');
INSERT INTO value_sets.value_set_version VALUES ('987ffe8a-27a8-11ec-9621-0242ac130002', '2021-01-01', '2021-12-31', '328b1fc4-27a8-11ec-9621-0242ac130002', 'draft', 'version 1', '2021-11-09', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('b76da030-6d7c-11ec-8279-29065e5912c2', '2022-01-01', '2023-01-01', 'b2bb62c0-6d7c-11ec-b1fa-71c6dd1818a3', 'active', '', '2022-01-04', 1, 'Status updated to active by Hao Sun on January 7th 2022, 11:44:32 am');
INSERT INTO value_sets.value_set_version VALUES ('b8a8dc20-6d78-11ec-be4d-17414cf6b148', '2022-01-01', '2023-01-01', '9ca0f4e0-6d78-11ec-8b39-799261a6657c', 'active', '', '2022-01-04', 1, 'Status updated to active by Stephen Weaver on January 7th 2022, 1:17:08 pm');
INSERT INTO value_sets.value_set_version VALUES ('4ca61a90-6d9d-11ec-bf8f-b1e7af44f5d2', '2022-01-04', '2023-01-04', '48ac0990-6d9d-11ec-8a9d-0504338216ef', 'active', '', '2022-01-04', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 3:23:46 pm');
INSERT INTO value_sets.value_set_version VALUES ('5232e3e0-6d79-11ec-8c80-3dd750fb8ea9', '2022-01-01', '2023-01-01', '4e607f20-6d79-11ec-98b0-918f8949cc42', 'active', '', '2022-01-04', 1, 'Status updated to active by Stephen Weaver on January 7th 2022, 1:25:23 pm');
INSERT INTO value_sets.value_set_version VALUES ('6719de00-6d81-11ec-a3ad-bb774fcfd5e7', '2022-01-01', '2023-01-01', '5f2287b0-6d81-11ec-bee7-e106dc640613', 'active', '', '2022-01-04', 1, 'Status updated to active by Hao Sun on January 7th 2022, 12:42:42 pm');
INSERT INTO value_sets.value_set_version VALUES ('c9c99f20-6d83-11ec-9816-f9531e40ab0f', '2022-01-01', '2023-01-01', 'c4587e80-6d83-11ec-ae12-57a69b13a7b5', 'active', '', '2022-01-04', 1, 'Status updated to active by Hao Sun on January 7th 2022, 12:49:58 pm');
INSERT INTO value_sets.value_set_version VALUES ('68c917d0-6d85-11ec-9b73-173b2c1e0f5a', '2022-01-01', '2023-01-01', '5ba2da00-6d85-11ec-b733-311671ebad29', 'active', '', '2022-01-04', 1, 'Status updated to active by Hao Sun on January 7th 2022, 12:54:27 pm');
INSERT INTO value_sets.value_set_version VALUES ('26e1b290-6d8b-11ec-b06c-ab4a70f9f045', '2022-01-01', '2023-01-01', '2040eb90-6d8b-11ec-bcc4-f7e61651b088', 'active', '', '2022-01-04', 1, 'Status updated to active by Hao Sun on January 7th 2022, 1:30:12 pm');
INSERT INTO value_sets.value_set_version VALUES ('0e6c91a0-635c-11ec-a9d1-2f9bdf584328', '2021-12-22', '2022-12-22', 'a757b6c0-635b-11ec-9eb7-612d55c4f5aa', 'active', '', '2021-12-22', 1, 'Status updated to active by Stephen Weaver on January 6th 2022, 2:00:23 pm');
INSERT INTO value_sets.value_set_version VALUES ('371d7850-6d8c-11ec-b9d9-f3c6146224bc', '2022-01-01', '2023-01-01', '341d30a0-6d8c-11ec-ac3e-fffa4d17d4a4', 'active', '', '2022-01-04', 1, 'Status updated to active by Hao Sun on January 7th 2022, 1:46:05 pm');
INSERT INTO value_sets.value_set_version VALUES ('c447c800-6343-11ec-9b51-4fc98501ea85', '2021-12-22', '2023-01-01', 'bfcb8eb0-6343-11ec-bd13-cbbf4db9fbeb', 'active', 'Production grade valueset using ReTool.', '2021-12-22', 1, 'Status updated to active by Stephen Weaver on January 7th 2022, 11:39:48 am');
INSERT INTO value_sets.value_set_version VALUES ('e6445e30-4c8e-11ec-9b89-3d7148967d21', '2021-01-01', '2021-12-31', 'e1ba9050-4c8e-11ec-99d0-6d735801ea9a', 'active', 'initial version', '2021-11-23', 1, 'Status updated to active by Hao Sun on January 7th 2022, 10:41:53 am');
INSERT INTO value_sets.value_set_version VALUES ('edf1c7d0-6d8b-11ec-a777-05d1227ee773', '2022-01-04', '2023-01-04', 'e9461a10-6d8b-11ec-92f6-4991e9e492ec', 'active', '', '2022-01-04', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 1:49:58 pm');
INSERT INTO value_sets.value_set_version VALUES ('557fc030-634d-11ec-87a6-c3c1dc9d48ae', '2021-12-22', '2022-12-22', '50ba21d0-634d-11ec-8615-fb597cd13462', 'active', '', '2021-12-22', 1, 'Status updated to active by Hao Sun on January 7th 2022, 10:53:31 am');
INSERT INTO value_sets.value_set_version VALUES ('074ce090-59ee-11ec-a5f9-ede488412e44', '2021-12-10', '2022-12-10', '035ba930-59ee-11ec-be32-7929feea4490', 'active', '', '2021-12-10', 0, 'Status updated to active by Hao Sun on January 7th 2022, 11:01:18 am');
INSERT INTO value_sets.value_set_version VALUES ('684b1f20-6d8e-11ec-8e4c-35786fa580cd', '2022-01-04', '2023-01-04', '64dde4d0-6d8e-11ec-a170-05e47613622b', 'active', 'SNOMED and ICD10 codes for the comorbid condition of fluid-electrolyte disorder', '2022-01-04', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 2:04:16 pm');
INSERT INTO value_sets.value_set_version VALUES ('5eca80d0-6d7a-11ec-afff-c93e25241607', '2022-01-01', '2023-01-01', '5b228770-6d7a-11ec-b87b-d7c29dbf0b8b', 'active', '', '2022-01-04', 1, 'Status updated to active by Stephen Weaver on January 7th 2022, 2:47:14 pm');
INSERT INTO value_sets.value_set_version VALUES ('37bf6530-6dae-11ec-8bcb-533e96f22a5a', '2022-01-04', '2023-01-04', '2af996e0-6dae-11ec-93e8-a5c70bfd1107', 'active', '', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('20f7e880-6364-11ec-a49b-ef00db410408', '2021-12-22', '2022-12-22', '18550ff0-6364-11ec-bee7-1b2cefdcd5ad', 'active', 'Protein kinase inhibitors ', '2021-12-22', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('8ea45d30-6415-11ec-bc6f-e74005669d8e', '2021-12-23', '2023-01-02', 'abd4ff40-6410-11ec-8daa-b365d151d4db', 'active', '', '2021-12-23', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('df47bb40-6dae-11ec-984c-bb9e6fde8fa7', '2022-01-04', '2023-01-04', 'd9d05910-6dae-11ec-a1e9-35e80f62d80b', 'active', '', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('e90ac880-6dbc-11ec-85e8-2768d3ecadc2', '2022-01-02', '2022-12-31', 'e2ea06f0-6dbc-11ec-bb58-8de119727b07', 'active', 'MVP ED Risk Model Generation', '2022-01-05', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('6c2e5ce0-52ec-11ec-b9db-4b5741b59f6a', '2021-12-01', NULL, '51801230-52ec-11ec-89f4-eb5726153df6', 'active', 'First version of valueset', '2021-12-01', 1, 'Status updated to active by Hao Sun on January 7th 2022, 11:06:54 am');
INSERT INTO value_sets.value_set_version VALUES ('717fd080-6353-11ec-bec4-cf27b3b51261', '2021-12-22', '2023-01-01', '6bda8080-6353-11ec-a426-01ccf2bd9dab', 'active', '', '2021-12-22', 1, 'Status updated to active by Hao Sun on January 7th 2022, 11:10:15 am');
INSERT INTO value_sets.value_set_version VALUES ('3a2d1c30-636e-11ec-998f-8970342a81b0', '2021-12-22', '2023-01-01', '2b6f9c90-636e-11ec-a609-b5614185f7a5', 'active', '', '2021-12-22', 1, 'Status updated to active by Hao Sun on January 7th 2022, 11:16:10 am');
INSERT INTO value_sets.value_set_version VALUES ('504a7600-6d93-11ec-83c6-5b12af31c27e', '2022-01-04', '2023-01-04', 'ce94fe50-6d8d-11ec-b97f-b976d95b97d2', 'active', '', '2022-01-04', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 2:22:46 pm');
INSERT INTO value_sets.value_set_version VALUES ('079dadc0-6d78-11ec-9b16-f1636a4e01b7', '2022-01-01', '2023-01-01', 'ffdb2310-6d77-11ec-b2ec-7d2ddcce13c5', 'active', '', '2022-01-04', 1, 'Status updated to active by Stephen Weaver on January 7th 2022, 12:24:51 pm');
INSERT INTO value_sets.value_set_version VALUES ('66cae7c0-6d8e-11ec-adf2-c94257d82fe7', '2022-01-01', '2023-01-01', '2e3d5dc0-6d8e-11ec-b97f-b976d95b97d2', 'active', '', '2022-01-04', 1, 'Status updated to active by Hao Sun on January 7th 2022, 1:53:35 pm');
INSERT INTO value_sets.value_set_version VALUES ('b54f6530-6daa-11ec-8de4-298c71d2b48d', '2022-01-04', '2023-01-04', 'aadd9900-6daa-11ec-9f7d-ddc3ed69f759', 'active', '', '2022-01-04', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 5:28:43 pm');
INSERT INTO value_sets.value_set_version VALUES ('345601d0-6db6-11ec-bdf5-d532fab59c4f', '2022-01-04', '2023-01-04', '2e1ef920-6db6-11ec-95f4-fdc633495259', 'active', '', '2022-01-04', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 6:21:39 pm');
INSERT INTO value_sets.value_set_version VALUES ('5daad360-6e53-11ec-a661-bf8941e42844', '2022-01-01', '2023-01-01', '57d28c80-6e53-11ec-a288-9761f3821129', 'active', '', '2022-01-05', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 6:49:54 pm');
INSERT INTO value_sets.value_set_version VALUES ('b8147000-6e57-11ec-a991-e358c9d3813d', '2022-01-01', '2022-01-01', 'b2fba890-6e57-11ec-8eff-a944250eaa2c', 'active', '', '2022-01-05', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 7:07:01 pm');
INSERT INTO value_sets.value_set_version VALUES ('2e47b270-6d98-11ec-94a4-43fe1cbd95b1', '2022-01-01', '2023-01-01', '29a50560-6d98-11ec-afbe-617680d9103a', 'active', '', '2022-01-04', 1, 'Status updated to active by Stephen Weaver on January 7th 2022, 5:34:45 pm');
INSERT INTO value_sets.value_set_version VALUES ('e2707f10-6d99-11ec-aca6-9f3bb3e4342a', '2022-01-01', '2023-01-01', 'bc157730-6d99-11ec-aa67-fdd64fa9f37f', 'active', '', '2022-01-04', 1, 'Status updated to active by Stephen Weaver on January 7th 2022, 5:40:15 pm');
INSERT INTO value_sets.value_set_version VALUES ('5e0b9110-6d8f-11ec-8a72-5d3bb70405c2', '2022-01-01', '2023-01-01', '58f38cf0-6d8f-11ec-b520-41f15c77ac39', 'active', '', '2022-01-04', 1, 'Status updated to active by Stephen Weaver on January 7th 2022, 6:10:35 pm');
INSERT INTO value_sets.value_set_version VALUES ('5dbf2e80-6d7e-11ec-a301-277758261289', '2022-01-01', '2023-01-01', '53d297e0-6d7e-11ec-9e94-f35e8c0249ad', 'active', '', '2022-01-04', 1, 'Status updated to active by Hao Sun on January 7th 2022, 5:13:09 pm');
INSERT INTO value_sets.value_set_version VALUES ('bce71d44-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'ca1f9350-6cff-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('bd529722-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '089e1c50-6d00-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('bdb98d56-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '1fe176a0-6d00-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('bdfedd66-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '3c5e8de0-6d00-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('be67fd82-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '53a0afb0-6d00-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c00b3a46-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '6c4d0fe0-6d00-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c0454718-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '53471430-6d93-11ec-9406-0d51e8580359', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c095ce36-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '8d2236a0-6d00-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c0f171dc-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'a846c2c0-6d00-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c166e6c4-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'bd83c5c0-6d00-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c1b81602-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'de9224f0-6d00-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c1f25fd8-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'fb519c60-6d00-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c22ca79c-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'ea625dc0-6d02-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c24fbee4-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '11192c20-6d01-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c27ec95a-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '55ce84d0-6d03-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c2aebf2a-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'c4f97e10-6d02-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c2dd2c48-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '6e885280-6d03-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c30b1d10-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'b070fe00-6d02-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c32f5c2a-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '873b6970-6d03-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c3a3afda-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '9ee044b0-6d03-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c3f4cbfe-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'b953d050-6d03-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c4173928-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'd6d2c730-6d03-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c4adf462-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'ec268680-6d03-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c55f8e0c-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '0e27aa20-6d04-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c5b10ea8-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '26ce6500-6d04-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c60da384-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '43dc71a0-6d04-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c7180efe-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '5789ace0-6d04-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c75f7424-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '68c39e30-6d04-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c7dea744-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '7f346640-6d04-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c88158c2-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '8c36a150-6d04-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c931dbb6-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', '9b8627c0-6d04-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('cb37d5a0-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'b0142610-6d04-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('cdb7b5a2-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'c466c910-6d04-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('cdfd5e68-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'd6a07720-6d04-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('ce7b3068-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'eb43f940-6d04-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('cf0e40e2-6d99-11ec-a98d-00163efae4c9', '2022-01-01', '2022-12-31', 'faf045b0-6d04-11ec-a5f0-91e137910fd5', 'active', 'MVP for ED risk model release', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('dccff9f0-6dad-11ec-9d25-45d59604686a', '2022-01-04', '2023-01-04', 'bb8afa60-6dad-11ec-aded-f11e9050236f', 'active', '', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('9995c830-6dae-11ec-b55a-f105f5c21ca0', '2022-01-04', '2023-01-04', '94055fc0-6dae-11ec-b3fb-734142b5f479', 'active', '', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('a52bbd80-6db8-11ec-a83e-a73f304d1e65', '2022-01-01', '2022-12-31', '98b92730-6413-11ec-9cfa-b7767fcb0438', 'active', 'MVP for ED risk model features', '2022-01-04', 1, NULL);
INSERT INTO value_sets.value_set_version VALUES ('c6807ee0-6d94-11ec-9533-5b425d4b75e7', '2022-01-04', '2023-01-04', 'c2099cc0-6d94-11ec-9ba4-09f608633acd', 'active', '', '2022-01-04', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 2:32:08 pm');
INSERT INTO value_sets.value_set_version VALUES ('ee27d8b0-6d9b-11ec-b0c2-55232efc5773', '2022-01-04', '2023-01-04', 'e99cd250-6d9b-11ec-97fc-0f785516332e', 'active', '', '2022-01-04', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 2:48:13 pm');
INSERT INTO value_sets.value_set_version VALUES ('c5f13b90-6db2-11ec-b0c5-07612a97300d', '2022-01-04', '2023-01-04', 'bf171510-6db2-11ec-bad0-8f90790692ae', 'active', '', '2022-01-04', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 5:48:55 pm');
INSERT INTO value_sets.value_set_version VALUES ('96646be0-6e52-11ec-9c2b-bbbba2505f61', '2022-01-01', '2023-01-01', '9309a820-6e52-11ec-9ea2-c7238c39a6c5', 'active', '', '2022-01-05', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 6:24:46 pm');
INSERT INTO value_sets.value_set_version VALUES ('42b77c90-6da7-11ec-9f83-1d5915becbdf', '2022-01-04', '2023-01-04', '3f7cc3f0-6da7-11ec-bd70-69b80260bb41', 'active', '', '2022-01-04', 1, 'Status updated to active by Stephen Weaver on January 7th 2022, 4:34:36 pm');
INSERT INTO value_sets.value_set_version VALUES ('fbf9fcf0-6e56-11ec-91d9-c1067f0b634c', '2022-01-01', '2023-01-01', 'f7f4ef70-6e56-11ec-843a-81f6989b647b', 'active', '', '2022-01-05', 1, 'Status updated to active by Katelin Brown on January 7th 2022, 6:56:19 pm');
INSERT INTO value_sets.value_set_version VALUES ('82ef0ff0-6d96-11ec-96e9-39ac2ca38464', '2022-01-01', '2023-01-01', '7e6baab0-6d96-11ec-bd52-17147d6e7f73', 'active', '', '2022-01-04', 1, 'Status updated to active by Stephen Weaver on January 7th 2022, 5:15:35 pm');


-- Completed on 2022-01-11 16:05:18 MST

--
-- PostgreSQL database dump complete
--

