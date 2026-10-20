import sys, json, tempfile, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import freshdesk_activity_dashboard as d
A = '019fa0df-0c00-8796-8ae5-0094c4fdf515'
B = '019fa0df-0c00-8796-8ae5-0094c4fdf516'

class HubSpot:

    def __init__(self, ambiguous=False, failed=False):
        self.calls = []
        self.ambiguous = ambiguous
        self.failed = failed

    def request(self, method, path, payload):
        self.calls.append((path, payload))
        if self.failed:
            return (403, {}, '')
        if path.endswith('/deals/search'):
            f = payload['filterGroups'][0]['filters'][0]
            assert f['operator'] == 'IN' and len(f['values']) <= 50
            values = f['values']
            return (200, {'results': [{'id': str(i + 1), 'properties': {f['propertyName']: value, 'hs_lastmodifieddate': '2026-10-20'}} for (i, value) in enumerate(values)]}, '')
        if path.endswith('/deals/companies/batch/read'):
            return (200, {'results': [{'from': {'id': v['id']}, 'to': [{'id': '20'}] + ([{'id': '21'}] if self.ambiguous else [])} for v in payload['inputs']]}, '')
        if path.endswith('/companies/batch/read'):
            return (200, {'results': [{'id': v['id'], 'properties': {'name': 'Aceme Industries'}} for v in payload['inputs']]}, '')
        raise AssertionError(path)

class NoWrites:

    def update_ticket(self, *args):
        raise AssertionError('Must not write to Freshdesk')

class CompanyResolutionTests(unittest.TestCase):

    def ticket(self, identifier=A):
        return {'id': 18669, 'custom_fields': {'cf_company_id880506': identifier}}

    def test_external_fields(self):
        self.assertEqual(d.ticket_external_company_id(self.ticket()), A)
        self.assertEqual(d.ticket_external_company_id({'custom_fields': {'cf_company_id880506': '-', 'cf_company_id': '123'}}), '123')
        for value in ['', None, 'Unknown', '-', 'not-an-id']:
            self.assertEqual(d.ticket_external_company_id(self.ticket(value)), '')

    def test_ticket_company_precedence(self):
        self.assertEqual(d.freshdesk_company_id_for_ticket({'company_id': 42, 'requester_id': 1}, {1: '12'}), '42')
        self.assertEqual(d.freshdesk_company_id_for_ticket({'company_id': None, 'requester_id': 1}, {1: '12'}), '12')

    def test_exact_resolution_and_deduplication(self):
        client = HubSpot()
        errors = []
        result = d.resolve_hubspot_companies_by_external_id([self.ticket(), self.ticket()], client, errors)
        self.assertEqual(result[A]['name'], 'Aceme Industries')
        self.assertEqual(result[A]['deal_ids'], ['1'])
        self.assertEqual(errors, [])
        self.assertEqual(len(client.calls), 3)

    def test_ambiguous_and_failed(self):
        errors = []
        self.assertEqual(d.resolve_hubspot_companies_by_external_id([self.ticket()], HubSpot(ambiguous=True), errors), {})
        self.assertTrue(errors)
        errors = []
        self.assertEqual(d.resolve_hubspot_companies_by_external_id([self.ticket()], HubSpot(failed=True), errors), {})
        self.assertTrue(errors)
        self.assertEqual(d.resolve_hubspot_companies_by_external_id([self.ticket()], None, []), {})

    def test_enrichment_without_requester_company(self):
        with tempfile.TemporaryDirectory() as folder:
            (names, urls, counts, _) = d.enrich_hubspot_context(client=NoWrites(), tickets=[self.ticket()], ticket_map={}, output_dir=Path(folder), hubspot_client=HubSpot(), hubspot_ui_domain='app.hubspot.com', hubspot_portal_id='1', hubspot_deal_url_field='cf_hubspot_deal_url', fetch_errors=[])
            self.assertEqual(names[18669], 'Aceme Industries')
            self.assertTrue(urls[18669].endswith('/1'))
            self.assertEqual(counts[18669], 1)

    def test_existing_company_and_deal_are_preserved(self):
        ticket = self.ticket()
        ticket['_freshdesk_company_name'] = 'Existing Company'
        ticket['custom_fields']['cf_hubspot_deal_url'] = 'https://app.hubspot.com/contacts/1/record/0-3/99'
        client = HubSpot()
        with tempfile.TemporaryDirectory() as folder:
            (names, urls, counts, _) = d.enrich_hubspot_context(client=NoWrites(), tickets=[ticket], ticket_map={}, output_dir=Path(folder), hubspot_client=client, hubspot_ui_domain='app.hubspot.com', hubspot_portal_id='1', hubspot_deal_url_field='cf_hubspot_deal_url', fetch_errors=[])
            self.assertNotIn(18669, names)
            self.assertTrue(urls[18669].endswith('/99'))
            self.assertEqual(client.calls, [])

    def test_paginated_search(self):

        class Paged(HubSpot):

            def request(self, method, path, payload):
                if path.endswith('/deals/search'):
                    if 'after' not in payload:
                        return (200, {'results': [{'id': '1', 'properties': {'deal_companyid_v2': A}}], 'paging': {'next': {'after': 'next-page'}}}, '')
                    return (200, {'results': [{'id': '2', 'properties': {'deal_companyid_v2': B}}]}, '')
                return super().request(method, path, payload)
        contexts = d.resolve_hubspot_companies_by_external_id([self.ticket(A), self.ticket(B)], Paged(), [])
        self.assertEqual(set(contexts), {A, B})
        self.assertEqual(contexts[B]['deal_ids'], ['2'])
if __name__ == '__main__':
    unittest.main()
