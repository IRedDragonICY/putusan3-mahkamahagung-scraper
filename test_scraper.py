import unittest
import sys
from MahkamahAgungScraper import MahkamahAgungScraper

class TestMahkamahAgungScraperLive(unittest.TestCase):

    def setUp(self):
        self.scraper = MahkamahAgungScraper()
        self.maxDiff = None

    def test_extract_last_page_from_list_pengadilan(self):
        target_url = "https://putusan3.mahkamahagung.go.id/pengadilan/index/ditjen/umum.html"
        print(f"\n[Test: Last Page - List] Fetching live data from: {target_url}")
        live_html = None
        try:
            live_html = self.scraper._fetch_page(1, target_url)
        except Exception as e:
            self.fail(f"Failed to fetch live HTML from {target_url}: {e}")

        self.assertIsNotNone(live_html, f"Received None HTML from {target_url}")
        print("[Test: Last Page - List] Live HTML fetched. Parsing last page...")
        last_page = self.scraper._get_last_page(live_html)

        print(f"[Test: Last Page - List] Parsed last page: {last_page}")
        self.assertIsInstance(last_page, int)
        self.assertGreaterEqual(last_page, 1)
        print("[Test: Last Page - List] Assertions passed.")

    def test_extract_last_page_from_one_of_pengadilan(self):
        target_url = "https://putusan3.mahkamahagung.go.id/direktori/pengadilan/pn-airmadidi.html"
        print(f"\n[Test: Last Page - Detail] Fetching live data from: {target_url}")
        live_html = None
        try:
            live_html = self.scraper._fetch_page(1, target_url)
        except Exception as e:
            self.fail(f"Failed to fetch live HTML from {target_url}: {e}")

        self.assertIsNotNone(live_html, f"Received None HTML from {target_url}")
        print("[Test: Last Page - Detail] Live HTML fetched. Parsing last page (expecting low number or 1)...")
        last_page = self.scraper._get_last_page(live_html)

        print(f"[Test: Last Page - Detail] Parsed last page: {last_page}")
        self.assertIsInstance(last_page, int)
        self.assertGreaterEqual(last_page, 1)
        print("[Test: Last Page - Detail] Assertions passed.")

    def test_extract_court_list(self):
        target_url = "https://putusan3.mahkamahagung.go.id/pengadilan/index/ditjen/umum.html"
        print(f"\n[Test: Court List Parse] Fetching court list data from: {target_url}")
        live_html = None
        try:
            live_html = self.scraper._fetch_page(1, target_url)
        except Exception as e:
            self.fail(f"Failed to fetch live HTML from {target_url}: {e}")

        self.assertIsNotNone(live_html, f"Received None HTML from {target_url}")
        print("[Test: Court List Parse] Live HTML fetched. Parsing court data...")

        court_data = self.scraper._parse_data(live_html, 1)

        self.assertIsNotNone(court_data, "Court data should not be None")
        self.assertIsInstance(court_data, list, "Court data should be a list")
        print(f"[Test: Court List Parse] Found {len(court_data)} courts on page 1.")
        self.assertGreater(len(court_data), 0, "Court data list should not be empty")

        if court_data:
            first_court = court_data[0]
            print(f"[Test: Court List Parse] Verifying structure of first court: {first_court.get('nama_pengadilan', 'N/A')}")
            self.assertIn("nama_pengadilan", first_court)
            self.assertIsInstance(first_court["nama_pengadilan"], str)
            self.assertIn("link_pengadilan", first_court)
            self.assertTrue(isinstance(first_court["link_pengadilan"], str) or first_court["link_pengadilan"] is None)
            self.assertIn("pengadilan_tinggi", first_court)
            self.assertIsInstance(first_court["pengadilan_tinggi"], str)
            self.assertIn("link_pengadilan_tinggi", first_court)
            self.assertTrue(isinstance(first_court["link_pengadilan_tinggi"], str) or first_court["link_pengadilan_tinggi"] is None)
            self.assertIn("provinsi", first_court)
            self.assertIsInstance(first_court["provinsi"], str)
            self.assertIn("jumlah_raw", first_court)
            self.assertIsInstance(first_court["jumlah_raw"], str)
            self.assertIn("jumlah_putusan", first_court)
            self.assertTrue(isinstance(first_court["jumlah_putusan"], int) or first_court["jumlah_putusan"] is None)
            self.assertIn("jumlah_publikasi", first_court)
            self.assertTrue(isinstance(first_court["jumlah_publikasi"], int) or first_court["jumlah_publikasi"] is None)

        print("[Test: Court List Parse] Assertions passed.")

    def test_get_court_list(self):
        target_url = "https://putusan3.mahkamahagung.go.id/pengadilan/index/ditjen/umum.html"
        print(f"\n[Test: Get Court List Method] Testing get_court_list with URL: {target_url} (max_pages=1)")

        courts = self.scraper.get_court_list(url=target_url, max_pages=1)

        self.assertIsNotNone(courts, "Court list result should not be None")
        self.assertIsInstance(courts, list, "Court list result should be a list")
        print(f"[Test: Get Court List Method] Fetched {len(courts)} courts using get_court_list().")
        self.assertGreater(len(courts), 0, "Court list result should not be empty")

        if courts:
            court = courts[0]
            print(f"[Test: Get Court List Method] Sample court: {court.get('nama_pengadilan', 'N/A')}")
            self.assertIn("nama_pengadilan", court)
            self.assertIn("link_pengadilan", court)
            self.assertIn("pengadilan_tinggi", court)
            self.assertIn("provinsi", court)
            self.assertIn("jumlah_putusan", court)

        print("[Test: Get Court List Method] Test passed.")

    def test_extract_court_yearly_decisions(self):
        test_court_code = "pn-airmadidi"
        target_url = f"https://putusan3.mahkamahagung.go.id/direktori/periode/tahunjenis/putus/pengadilan/{test_court_code}.html"

        print(f"\n[Test: Yearly Decisions] Testing get_court_yearly_decisions for court code: {test_court_code}")
        print(f"[Test: Yearly Decisions] Attempt 1: Fetching via URL: {target_url}")

        yearly_data_url = []
        try:
            yearly_data_url = self.scraper.get_court_yearly_decisions(url=target_url)
        except Exception as e:
            self.fail(f"[Test: Yearly Decisions] Failed to fetch yearly decision data via URL: {e}")

        self.assertIsNotNone(yearly_data_url, "[Test: Yearly Decisions] Yearly decision data (URL) should not be None")
        self.assertIsInstance(yearly_data_url, list, "[Test: Yearly Decisions] Yearly decision data (URL) should be a list")
        print(f"[Test: Yearly Decisions] Fetched {len(yearly_data_url)} yearly records via URL.")
        self.assertGreater(len(yearly_data_url), 0, "[Test: Yearly Decisions] Yearly decision data (URL) should not be empty for this court")

        print("[Test: Yearly Decisions] Displaying all results fetched (from URL method):")
        for i, item in enumerate(yearly_data_url):
            print(f"  Record {i+1}: Year={item.get('year', 'N/A')}, Count={item.get('decision_count', 'N/A')}, Link={item.get('link', 'N/A')}")
            self.assertIsInstance(item, dict, f"[Test: Yearly Decisions] Item {i} should be a dict")
            self.assertIn("year", item, f"[Test: Yearly Decisions] Item {i} missing 'year'")
            self.assertIsInstance(item["year"], str, f"[Test: Yearly Decisions] Item {i} 'year' should be str")
            self.assertIn("decision_count", item, f"[Test: Yearly Decisions] Item {i} missing 'decision_count'")
            self.assertIsInstance(item["decision_count"], int, f"[Test: Yearly Decisions] Item {i} 'decision_count' should be int")
            self.assertIn("link", item, f"[Test: Yearly Decisions] Item {i} missing 'link'")
            self.assertIsInstance(item["link"], str, f"[Test: Yearly Decisions] Item {i} 'link' should be str")

        print(f"[Test: Yearly Decisions] Attempt 2: Fetching via court_code: {test_court_code} (for comparison)")
        yearly_data_code = []
        try:
            yearly_data_code = self.scraper.get_court_yearly_decisions(court_code=test_court_code)
        except Exception as e:
            self.fail(f"[Test: Yearly Decisions] Failed to fetch yearly decision data via court_code: {e}")

        self.assertIsNotNone(yearly_data_code, "[Test: Yearly Decisions] Yearly decision data (code) should not be None")
        self.assertIsInstance(yearly_data_code, list, "[Test: Yearly Decisions] Yearly decision data (code) should be a list")
        print(f"[Test: Yearly Decisions] Fetched {len(yearly_data_code)} yearly records via court_code.")

        print("[Test: Yearly Decisions] Comparing results from URL and court_code methods...")
        self.assertEqual(len(yearly_data_url), len(yearly_data_code), "[Test: Yearly Decisions] Number of records from URL and court_code methods should match")
        self.assertListEqual(yearly_data_url, yearly_data_code, "[Test: Yearly Decisions] Content of records from URL and court_code methods should be identical")

        print("[Test: Yearly Decisions] Assertions and comparison passed.")


if __name__ == '__main__':
    print("Running Mahkamah Agung Scraper Live Tests...")
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestMahkamahAgungScraperLive))
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    runner.run(suite)