-- Create concept map for testing
insert into concept_maps.concept_map
(uuid, name, title, publisher, author, purpose, description, created_date, experimental, source_value_set_uuid, target_value_set_uuid)
values
('cb0df6d5-816c-4571-9a55-25c8dc324f1d', 'TestNewVersionConceptMap', 'Test New Version Concept Map',
 'Project Ronin', 'Rey Johnson', 'integration tests',
 'Concept map from two test terminologies to test behavior for a new version of a concept map', '2022-10-27', true,
 'a68bb401-2237-4a44-87f8-95943a390797', 'cd9f70de-53c1-4a48-aac8-7db97f7408c4')
on conflict do nothing;

insert into concept_maps.concept_map_version
(uuid, concept_map_uuid, description, comments, status, created_date, version, source_value_set_version_uuid, target_value_set_version_uuid)
values
('6a924772-e2bb-463c-a8e5-7de60a71321a', 'cb0df6d5-816c-4571-9a55-25c8dc324f1d', 'initial version', '', 'active', '2022-10-27', 1, '0a2940ec-c397-4969-907f-2c99ab7c5933', 'ddf0d42a-2e03-44e0-ac0f-565e3f9ed3df')
on conflict do nothing;

insert into concept_maps.source_concept
(uuid, code, display, system, map_status, concept_map_version_uuid, assigned_mapper, assigned_reviewer, no_map, reason_for_no_map, mapping_group)
values
('823a849e-df82-4262-bf6c-34a031d6c525', 'A', 'Source Code A', '3df509e6-6b37-4153-bf31-f9afcda18f4a', 'reviewed',
    '6a924772-e2bb-463c-a8e5-7de60a71321a', '915493ef-6d3e-4388-892e-662bedbde652', 'acfb20b1-361e-4642-bd48-090a8ad93e06', null, null, null),
('fd8241dc-d3c5-4904-a183-78c65b079bec', 'B', 'Source Code B', '3df509e6-6b37-4153-bf31-f9afcda18f4a', 'reviewed',
    '6a924772-e2bb-463c-a8e5-7de60a71321a', '915493ef-6d3e-4388-892e-662bedbde652', 'acfb20b1-361e-4642-bd48-090a8ad93e06', null, null, null),
('ea076675-d920-465e-851e-56c286e1a8f7', 'C', 'Source Code C', '3df509e6-6b37-4153-bf31-f9afcda18f4a', 'reviewed',
    '6a924772-e2bb-463c-a8e5-7de60a71321a', '915493ef-6d3e-4388-892e-662bedbde652', 'acfb20b1-361e-4642-bd48-090a8ad93e06', null, null, null),
('b24e8b98-74bd-4a0c-8ee7-0d3c04cade01', 'D', 'Source Code D', '3df509e6-6b37-4153-bf31-f9afcda18f4a', 'reviewed',
    '6a924772-e2bb-463c-a8e5-7de60a71321a', '915493ef-6d3e-4388-892e-662bedbde652', 'acfb20b1-361e-4642-bd48-090a8ad93e06', null, null, null),
('1be4943a-18bb-4b99-ab26-7df47c479c1e', 'E', 'Source Code E', '3df509e6-6b37-4153-bf31-f9afcda18f4a', 'reviewed',
    '6a924772-e2bb-463c-a8e5-7de60a71321a', '915493ef-6d3e-4388-892e-662bedbde652', 'acfb20b1-361e-4642-bd48-090a8ad93e06', null, null, null)
on conflict do nothing;

insert into concept_maps.concept_relationship
(uuid, source_concept_uuid, relationship_code_uuid, target_concept_code, target_concept_display,
 target_concept_system, target_concept_system_version_uuid, review_status, created_date, reviewed_date, author)
values
('f6fb9f3a-6790-4751-b14e-9735440fa5e6', '823a849e-df82-4262-bf6c-34a031d6c525', 'f2a20235-bd9d-4f6a-8e78-b3f41f97d07f', '1', 'Target Code 1', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping',
 '518582d7-dd86-4da1-93fb-ab46f027cfe0', 'reviewed', '2022-10-27', '2022-10-27', '915493ef-6d3e-4388-892e-662bedbde652'),
('94b69a73-a24b-4d0c-aa19-0d73ef7185ca', 'fd8241dc-d3c5-4904-a183-78c65b079bec', 'f2a20235-bd9d-4f6a-8e78-b3f41f97d07f', '2', 'Target Code 2', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping',
 '518582d7-dd86-4da1-93fb-ab46f027cfe0', 'reviewed', '2022-10-27', '2022-10-27', '915493ef-6d3e-4388-892e-662bedbde652'),
('3091d18b-9fdf-4081-94c4-c315847963d6', 'ea076675-d920-465e-851e-56c286e1a8f7', 'f2a20235-bd9d-4f6a-8e78-b3f41f97d07f', '3', 'Target Code 3', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping',
 '518582d7-dd86-4da1-93fb-ab46f027cfe0', 'reviewed', '2022-10-27', '2022-10-27', '915493ef-6d3e-4388-892e-662bedbde652'),
('97955845-417b-47e7-a210-a3afa31ddfac', 'b24e8b98-74bd-4a0c-8ee7-0d3c04cade01', 'f2a20235-bd9d-4f6a-8e78-b3f41f97d07f', '4', 'Target Code 4', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping',
 '518582d7-dd86-4da1-93fb-ab46f027cfe0', 'reviewed', '2022-10-27', '2022-10-27', '915493ef-6d3e-4388-892e-662bedbde652'),
('726edb89-b363-446d-9777-f6c05d813c1d', '1be4943a-18bb-4b99-ab26-7df47c479c1e', 'f2a20235-bd9d-4f6a-8e78-b3f41f97d07f', '5', 'Target Code 5', 'http://projectronin.io/FHIR/CodeSystems/testTargetForMapping',
 '518582d7-dd86-4da1-93fb-ab46f027cfe0', 'reviewed', '2022-10-27', '2022-10-27', '915493ef-6d3e-4388-892e-662bedbde652')
on conflict do nothing;