import unittest

from app.app import create_app

import app.concept_maps.rxnorm_mapping_models


class TestRxNormMappingModels(unittest.TestCase):
    def setUp(self) -> None:
        # self.conn = get_db()
        self.app = create_app()
        self.app.config.update(
            {
                "TESTING": True,
            }
        )
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        # Commented out standard connection rollback because these endpoints do not
        # open a database connection

        # self.conn.rollback()
        # self.conn.close()
        pass

    def test_parsing_logic(self):
        """Test that we can parse the RxNorm codes out of a raw source code"""
        example_input_1 = "{[{59790, urn:oid:2.16.840.1.113883.6.208}, {6754, http://www.nlm.nih.gov/research/umls/rxnorm}, {103755, http://www.nlm.nih.gov/research/umls/rxnorm}], meperidine PF (DEMEROL) injection (50 mg/mL vial)}"
        example_input_2 = "{[{M03AC09, http://www.whocc.no/atc}, {21727, urn:oid:2.16.840.1.113883.6.208}, {32521, http://www.nlm.nih.gov/research/umls/rxnorm}, {68139, http://www.nlm.nih.gov/research/umls/rxnorm}, {151718, http://www.nlm.nih.gov/research/umls/rxnorm}, {1234995, http://www.nlm.nih.gov/research/umls/rxnorm}, {1242617, http://www.nlm.nih.gov/research/umls/rxnorm}], rocuronium (ZEMURON) IV 10 mg/mL}"
        example_input_3 = "{[{N06AB03, http://www.whocc.no/atc}, {46219, urn:oid:2.16.840.1.113883.6.208}, {4493, http://www.nlm.nih.gov/research/umls/rxnorm}, {58827, http://www.nlm.nih.gov/research/umls/rxnorm}, {227224, http://www.nlm.nih.gov/research/umls/rxnorm}, {248642, http://www.nlm.nih.gov/research/umls/rxnorm}, {352004, http://www.nlm.nih.gov/research/umls/rxnorm}, {352940, http://www.nlm.nih.gov/research/umls/rxnorm}, {639464, http://www.nlm.nih.gov/research/umls/rxnorm}, {647556, http://www.nlm.nih.gov/research/umls/rxnorm}, {799023, http://www.nlm.nih.gov/research/umls/rxnorm}, {2532163, http://www.nlm.nih.gov/research/umls/rxnorm}], FLUoxetine (PROzac) tablet}"

        parsed_example_1 = (
            app.concept_maps.rxnorm_mapping_models.parse_rxnorm_codes_from_source_code(
                example_input_1
            )
        )

        self.assertListEqual(["6754", "103755"], parsed_example_1)

        parsed_example_2 = (
            app.concept_maps.rxnorm_mapping_models.parse_rxnorm_codes_from_source_code(
                example_input_2
            )
        )

        self.assertListEqual(
            ["32521", "68139", "151718", "1234995", "1242617"], parsed_example_2
        )

        parsed_example_3 = (
            app.concept_maps.rxnorm_mapping_models.parse_rxnorm_codes_from_source_code(
                example_input_3
            )
        )

        self.assertListEqual(
            [
                "4493",
                "58827",
                "227224",
                "248642",
                "352004",
                "352940",
                "639464",
                "647556",
                "799023",
                "2532163",
            ],
            parsed_example_3,
        )

    def test_get_rxnorm_data_for_source_code(self):

        example_input_1 = "{[{59790, urn:oid:2.16.840.1.113883.6.208}, {6754, http://www.nlm.nih.gov/research/umls/rxnorm}, {103755, http://www.nlm.nih.gov/research/umls/rxnorm}], meperidine PF (DEMEROL) injection (50 mg/mL vial)}"

        example_1_response = self.client.post(
            "/ConceptMaps/reference_data/RxNorm",
            json=({"source_code": example_input_1}),
        )
        data = example_1_response.get_json()
        table_data = data.get("rxnorm_info")
        obsolete_count = data.get("obsolete_count")

        self.assertEqual(example_1_response.status_code, 200)
        self.assertEqual(obsolete_count, 0)
        self.assertListEqual(
            table_data,
            [
                {
                    "name": "meperidine",
                    "rxcui": "6754",
                    "status": "Active",
                    "tty": "IN",
                },
                {
                    "name": "meperidine hydrochloride",
                    "rxcui": "103755",
                    "status": "Active",
                    "tty": "PIN",
                },
            ],
        )

        example_input_2 = "{[{M03AC09, http://www.whocc.no/atc}, {21727, urn:oid:2.16.840.1.113883.6.208}, {32521, http://www.nlm.nih.gov/research/umls/rxnorm}, {68139, http://www.nlm.nih.gov/research/umls/rxnorm}, {151718, http://www.nlm.nih.gov/research/umls/rxnorm}, {1234995, http://www.nlm.nih.gov/research/umls/rxnorm}, {1242617, http://www.nlm.nih.gov/research/umls/rxnorm}], rocuronium (ZEMURON) IV 10 mg/mL}"

        example_2_response = self.client.post(
            "/ConceptMaps/reference_data/RxNorm",
            json=({"source_code": example_input_2}),
        )
        data = example_2_response.get_json()
        table_data = data.get("rxnorm_info")
        obsolete_count = data.get("obsolete_count")

        self.assertEqual(example_2_response.status_code, 200)
        self.assertEqual(obsolete_count, 2)
        self.assertListEqual(
            table_data,
            [
                {
                    "name": "rocuronium bromide",
                    "rxcui": "32521",
                    "status": "Active",
                    "tty": "PIN",
                },
                {
                    "name": "rocuronium",
                    "rxcui": "68139",
                    "status": "Active",
                    "tty": "IN",
                },
                {
                    "name": "rocuronium bromide 10 MG/ML Injectable Solution",
                    "rxcui": "1234995",
                    "status": "Active",
                    "tty": "SCD",
                },
            ],
        )
