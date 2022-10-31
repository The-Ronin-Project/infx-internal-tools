--
-- Create test source system for mapping
--

-- First version
insert into public.terminology_versions
(uuid, terminology, version, fhir_uri, is_standard, fhir_terminology)
values
('3df509e6-6b37-4153-bf31-f9afcda18f4a', 'Test Source for Mapping', 1, 'http://projectronin.io/FHIR/CodeSystems/testSourceForMapping', false, false)
on conflict do nothing;

-- Populate with values
insert into custom_terminologies.code
(uuid, code, display, terminology_version)
values
('d212ab58-e70d-4141-83a9-720917982793', 'A', 'Source Code A', '3df509e6-6b37-4153-bf31-f9afcda18f4a'),
('a9b102ce-068d-407f-b3d6-aea0cff9f383', 'B', 'Source Code B (removed after v1)', '3df509e6-6b37-4153-bf31-f9afcda18f4a'),
('e366f453-99df-408f-8bd4-3fe1a8e54f97', 'C', 'Source Code C', '3df509e6-6b37-4153-bf31-f9afcda18f4a'),
('cb9efbd7-d9bd-47b2-86e6-5a77ee183090', 'D', 'Source Code D', '3df509e6-6b37-4153-bf31-f9afcda18f4a'),
('8c606d7d-994c-4c65-badc-97256b4d02a9', 'E', 'Source Code E', '3df509e6-6b37-4153-bf31-f9afcda18f4a')
on conflict do nothing;

-- Second version
-- Includes a new code and a deprecated code
insert into public.terminology_versions
(uuid, terminology, version, fhir_uri, is_standard, fhir_terminology)
values
('2bcbd79f-97e0-4a92-bc3f-0020b6401b77', 'Test Source for Mapping', 2, 'http://projectronin.io/FHIR/CodeSystems/testSourceForMapping', false, false)
on conflict do nothing;

-- Populate with values
insert into custom_terminologies.code
(uuid, code, display, terminology_version)
values
('5bc4e5e6-2791-4f74-abda-7456a0d288ab', 'A', 'Source Code A', '2bcbd79f-97e0-4a92-bc3f-0020b6401b77'),
('dcf25037-86da-4e3d-b3b8-b3fdde02014b', 'C', 'Source Code C', '2bcbd79f-97e0-4a92-bc3f-0020b6401b77'),
('2b8cbd82-1563-4273-af78-58f358707c68', 'D', 'Source Code D', '2bcbd79f-97e0-4a92-bc3f-0020b6401b77'),
('e014867e-2332-4965-83a2-ac656bdb8c15', 'E', 'Source Code E', '2bcbd79f-97e0-4a92-bc3f-0020b6401b77'),
('988b3667-1f03-4015-bace-a76d34bc7d55', 'F', 'Source Code F (new in v2)', '2bcbd79f-97e0-4a92-bc3f-0020b6401b77')
on conflict do nothing;

--
-- Create test target system for mapping
--

-- First version
insert into public.terminology_versions
(uuid, terminology, version, fhir_uri, is_standard, fhir_terminology)
values
('518582d7-dd86-4da1-93fb-ab46f027cfe0', 'Test Target for Mapping', 1, 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping', false, false)
on conflict do nothing;

-- Populate with values
insert into custom_terminologies.code
(uuid, code, display, terminology_version)
values
('2397ea54-0361-4de0-820a-86876936accb', '1', 'Target Code 1', '518582d7-dd86-4da1-93fb-ab46f027cfe0'),
('198ee53a-1ddf-4ffb-8d2b-6a613371cb4f', '2', 'Target Code 2', '518582d7-dd86-4da1-93fb-ab46f027cfe0'),
('b9344791-71ba-4038-b329-c8163d27a277', '3', 'Target Code 3 (deprecated in v2)', '518582d7-dd86-4da1-93fb-ab46f027cfe0'),
('789f5ac2-1b8d-4969-8aee-bf0a1b1da91b', '4', 'Target Code 4', '518582d7-dd86-4da1-93fb-ab46f027cfe0'),
('1999cc31-d06c-4bf2-a3c6-c8aefad84be9', '5', 'Target Code 5', '518582d7-dd86-4da1-93fb-ab46f027cfe0')
on conflict do nothing;

-- Second Version
insert into public.terminology_versions
(uuid, terminology, version, fhir_uri, is_standard, fhir_terminology)
values
('3ef04487-c416-4379-960d-f321a83e5895', 'Test Target for Mapping', 2, 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping', false, false)
on conflict do nothing;

-- Populate with values
insert into custom_terminologies.code
(uuid, code, display, terminology_version)
values
('e410f1b8-097c-4dea-8e56-850e557de164', '1', 'Target Code 1', '3ef04487-c416-4379-960d-f321a83e5895'),
('5e6490a7-5f81-46b3-8fd2-b6d9520ae2a6', '2', 'Target Code 2', '3ef04487-c416-4379-960d-f321a83e5895'),
('e57129cd-2b82-4bdb-acf6-cdd0a4add529', '4', 'Target Code 4', '3ef04487-c416-4379-960d-f321a83e5895'),
('275b9ca1-e8ef-439e-b5c4-bc662a02555f', '5', 'Target Code 5', '3ef04487-c416-4379-960d-f321a83e5895'),
('af8f2bc3-28c0-44b6-9ead-e25eea38447c', '6', 'Target Code 6 (new in v2)', '3ef04487-c416-4379-960d-f321a83e5895')
on conflict do nothing;
