--
-- Value Set for Map Source v1
--
insert into value_sets.value_set
(uuid, name, title, publisher, contact, description, immutable, experimental, purpose, type)
values
('a68bb401-2237-4a44-87f8-95943a390797', 'TestSourceForMapping', 'Test Source for Mapping', 'Project Ronin', 'Rey Johnson', 'a test value set', false, true, 'testing', 'intensional')
on conflict do nothing;

insert into value_sets.value_set_version
(uuid, value_set_uuid, status, description, created_date, version)
values
('0a2940ec-c397-4969-907f-2c99ab7c5933', 'a68bb401-2237-4a44-87f8-95943a390797', 'retired', 'initial version', '2022-10-17', 1)
on conflict do nothing;

insert into value_sets.value_set_rule
(uuid, position, description, property, operator, value, include, value_set_version, terminology_version, rule_group)
values
('141637b6-ccee-4454-9f69-73f2cac55d41', 1, 'include Test Source for Mapping code system', 'include_entire_code_system', '=', 'true', true, '0a2940ec-c397-4969-907f-2c99ab7c5933', '3df509e6-6b37-4153-bf31-f9afcda18f4a', 1)
on conflict do nothing;

insert into value_sets.expansion
(uuid, vs_version_uuid, timestamp)
values
('156d1624-563f-11ed-98a6-cae22ef1ec3f', '0a2940ec-c397-4969-907f-2c99ab7c5933', '2022-10-27 15:21:44.687804+00')
on conflict do nothing;

insert into value_sets.expansion_member
(uuid, expansion_uuid, code, display, system, version)
values
('dde89314-0539-4d3e-9f55-7d8e7cc1d5bd', '156d1624-563f-11ed-98a6-cae22ef1ec3f', 'A', 'Source Code A', 'http://projectronin.io/FHIR/testSourceForMapping', 1),
('326a30ab-7ea3-442b-8182-3d15948220f5', '156d1624-563f-11ed-98a6-cae22ef1ec3f', 'B', 'Source Code B (removed after v1)', 'http://projectronin.io/FHIR/testSourceForMapping', 1),
('9500d196-44fa-4d82-9def-34d9e16e68a1', '156d1624-563f-11ed-98a6-cae22ef1ec3f', 'C', 'Source Code C', 'http://projectronin.io/FHIR/testSourceForMapping', 1),
('1386819f-83b8-4882-9bd5-ca1a57b56802', '156d1624-563f-11ed-98a6-cae22ef1ec3f', 'D', 'Source Code D', 'http://projectronin.io/FHIR/testSourceForMapping', 1),
('e416f705-71b3-4bdd-beb7-d2c327af27d4', '156d1624-563f-11ed-98a6-cae22ef1ec3f', 'E', 'Source Code E', 'http://projectronin.io/FHIR/testSourceForMapping', 1)
on conflict do nothing;

--
-- Value Set for Map Source v2
--
insert into value_sets.value_set_version
(uuid, value_set_uuid, status, description, created_date, version)
values
('e1b275e5-cb7b-4130-a8a0-a9e4701d8af4', 'a68bb401-2237-4a44-87f8-95943a390797', 'active', 'new version for testing', '2022-10-17', 2)
on conflict do nothing;

insert into value_sets.value_set_rule
(uuid, position, description, property, operator, value, include, value_set_version, terminology_version, rule_group)
values
('e5b00899-248d-4ccc-8fca-96769744e167', 1, 'include Test Source for Mapping code system', 'include_entire_code_system', '=', 'true',
 true, 'e1b275e5-cb7b-4130-a8a0-a9e4701d8af4', '2bcbd79f-97e0-4a92-bc3f-0020b6401b77', 1)
on conflict do nothing;

insert into value_sets.expansion
(uuid, vs_version_uuid, timestamp)
values
('556cadae-5665-11ed-8a8e-cae22ef1ec3f', 'e1b275e5-cb7b-4130-a8a0-a9e4701d8af4', '2022-10-27 20:07:54.322487+00')
on conflict do nothing;

insert into value_sets.expansion_member
(uuid, expansion_uuid, code, display, system, version)
values
('80cefb90-fc66-4734-b5b1-c762c249dbd5', '556cadae-5665-11ed-8a8e-cae22ef1ec3f', 'A', 'Source Code A', 'http://projectronin.io/FHIR/testSourceForMapping', 2),
('dcb8b7b3-2e70-4870-8a8a-245b7b4f6506', '556cadae-5665-11ed-8a8e-cae22ef1ec3f', 'C', 'Source Code C', 'http://projectronin.io/FHIR/testSourceForMapping', 2),
('66c26962-93e8-434b-921a-66e636c64ab7', '556cadae-5665-11ed-8a8e-cae22ef1ec3f', 'D', 'Source Code D', 'http://projectronin.io/FHIR/testSourceForMapping', 2),
('3aeb0e84-01f9-4e2b-abd4-7c7318605a31', '556cadae-5665-11ed-8a8e-cae22ef1ec3f', 'E', 'Source Code E', 'http://projectronin.io/FHIR/testSourceForMapping', 2),
('903a42ce-c170-4508-a420-5606d15e190a', '556cadae-5665-11ed-8a8e-cae22ef1ec3f', 'F', 'Source Code F (new in v2)', 'http://projectronin.io/FHIR/testSourceForMapping', 2)
on conflict do nothing;

--
-- Value Set for Map Target v1
--
insert into value_sets.value_set
(uuid, name, title, publisher, contact, description, immutable, experimental, purpose, type)
values
('cd9f70de-53c1-4a48-aac8-7db97f7408c4', 'TestTargetForMapping', 'Test Target for Mapping', 'Project Ronin', 'Rey Johnson', 'a test value set', false, true, 'testing', 'intensional')
on conflict do nothing;

insert into value_sets.value_set_version
(uuid, value_set_uuid, status, description, created_date, version)
values
('ddf0d42a-2e03-44e0-ac0f-565e3f9ed3df', 'cd9f70de-53c1-4a48-aac8-7db97f7408c4', 'retired', 'initial version', '2022-10-17', 1)
on conflict do nothing;

insert into value_sets.value_set_rule
(uuid, position, description, property, operator, value, include, value_set_version, terminology_version, rule_group)
values
('6ecab502-5f6b-462d-a39d-56eaaa6dd835', 1, 'include Test Source for Mapping code system', 'include_entire_code_system', '=', 'true', true, 'ddf0d42a-2e03-44e0-ac0f-565e3f9ed3df', '518582d7-dd86-4da1-93fb-ab46f027cfe0', 1)
on conflict do nothing;

insert into value_sets.expansion
(uuid, vs_version_uuid, timestamp)
values
('356fc344-5645-11ed-98a6-cae22ef1ec3f', 'ddf0d42a-2e03-44e0-ac0f-565e3f9ed3df', '2022-10-27 16:17:56.760278+00')
on conflict do nothing;

insert into value_sets.expansion_member
(uuid, expansion_uuid, code, display, system, version)
values
('d1eb2d0c-a5e7-42e2-9939-edd0b7e756a3', '356fc344-5645-11ed-98a6-cae22ef1ec3f', '1', 'Target Code 1', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping', 1),
('60806664-0d94-43b6-81af-69f1f27e17a4', '356fc344-5645-11ed-98a6-cae22ef1ec3f', '2', 'Target Code 2', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping', 1),
('84050803-1a60-4f64-9f22-c03e78a99584', '356fc344-5645-11ed-98a6-cae22ef1ec3f', '3', 'Target Code 3', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping', 1),
('39ff8b7f-434b-4c77-aa9b-c0bee30e903f', '356fc344-5645-11ed-98a6-cae22ef1ec3f', '4', 'Target Code 4', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping', 1),
('46ee80b3-cc0f-41fd-b32c-41c141edbd8d', '356fc344-5645-11ed-98a6-cae22ef1ec3f', '5', 'Target Code 5', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping', 1)
on conflict do nothing;

--
-- Value Set for Map Target v2
--
insert into value_sets.value_set_version
(uuid, value_set_uuid, status, description, created_date, version)
values
('694397b0-ba3e-4d09-8d48-6693dc60d6d0', 'cd9f70de-53c1-4a48-aac8-7db97f7408c4', 'active', 'initial version', '2022-10-17', 2)
on conflict do nothing;

insert into value_sets.value_set_rule
(uuid, position, description, property, operator, value, include, value_set_version, terminology_version, rule_group)
values
('70a80d3b-29f5-4132-82e6-5f6a412ad207', 1, 'include Test Source for Mapping code system', 'include_entire_code_system', '=', 'true',
 true, '694397b0-ba3e-4d09-8d48-6693dc60d6d0', '3ef04487-c416-4379-960d-f321a83e5895', 1)
on conflict do nothing;

insert into value_sets.expansion
(uuid, vs_version_uuid, timestamp)
values
('526291cc-5666-11ed-8a8e-cae22ef1ec3f', '694397b0-ba3e-4d09-8d48-6693dc60d6d0', '2022-10-27 20:14:58.719809+00')
on conflict do nothing;

insert into value_sets.expansion_member
(uuid, expansion_uuid, code, display, system, version)
values
('7c3d49f4-a7a0-4155-972b-a3bcd87b0b29', '526291cc-5666-11ed-8a8e-cae22ef1ec3f', '1', 'Target Code 1', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping', 2),
('9d6693c5-db00-4fc4-8092-51edb51b9bb7', '526291cc-5666-11ed-8a8e-cae22ef1ec3f', '2', 'Target Code 2', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping', 2),
('3636e451-193b-4ac7-8b99-319ca7a8b384', '526291cc-5666-11ed-8a8e-cae22ef1ec3f', '4', 'Target Code 4', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping', 2),
('a766edb5-3a8a-4c95-b449-9ab85db83853', '526291cc-5666-11ed-8a8e-cae22ef1ec3f', '5', 'Target Code 5', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping', 2),
('30a5a089-9c44-416a-989b-0e0c003598aa', '526291cc-5666-11ed-8a8e-cae22ef1ec3f', '6', 'Target Code 6 (new in v2)', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping', 2)
on conflict do nothing;