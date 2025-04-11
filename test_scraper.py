import unittest
from MahkamahAgungScraper import MahkamahAgungScraper

class TestLiveLastPageExtraction(unittest.TestCase):

    def setUp(self):
        self.scraper = MahkamahAgungScraper()

    def test_extract_last_page_from_list_pengadilan(self):
        target_url = "https://putusan3.mahkamahagung.go.id/pengadilan/index/ditjen/umum.html"
        print(f"\nAttempting to fetch live data from: {target_url}")
        live_html = None
        try:
            live_html = self.scraper._fetch_page(1, target_url)
        except Exception as e:
            self.fail(f"Failed to fetch live HTML from {target_url}: {e}")

        self.assertIsNotNone(live_html, f"Received None HTML from {target_url}")
        print("Live HTML fetched successfully. Attempting to parse last page...")
        last_page = self.scraper._get_last_page(live_html)

        print(f"Parsed last page: {last_page}")
        print("Assertions passed.")

    def test_extract_last_page_from_one_of_pengadilan(self):
        target_url = "https://putusan3.mahkamahagung.go.id/pengadilan/profil/pengadilan/pn-airmadidi.html"
        print(f"\nAttempting to fetch live data from: {target_url}")
        live_html = None
        try:
            live_html = self.scraper._fetch_page(1, target_url)
        except Exception as e:
            self.fail(f"Failed to fetch live HTML from {target_url}: {e}")

        self.assertIsNotNone(live_html, f"Received None HTML from {target_url}")
        print("Live HTML fetched successfully. Attempting to parse last page...")
        last_page = self.scraper._get_last_page(live_html)

        print(f"Parsed last page: {last_page}")
        print("Assertions passed.")

    def test_extract_court_list(self):
        target_url = "https://putusan3.mahkamahagung.go.id/pengadilan/index/ditjen/umum.html"
        print(f"\nAttempting to fetch court list data from: {target_url}")
        live_html = None
        try:
            live_html = self.scraper._fetch_page(1, target_url)
        except Exception as e:
            self.fail(f"Failed to fetch live HTML from {target_url}: {e}")

        self.assertIsNotNone(live_html, f"Received None HTML from {target_url}")
        print("Live HTML fetched successfully. Attempting to parse court data...")
        
        court_data = self.scraper._parse_data(live_html, 1)
        
        self.assertIsNotNone(court_data, "Court data should not be None")
        self.assertIsInstance(court_data, list, "Court data should be a list")
        self.assertGreater(len(court_data), 0, "Court data should not be empty")
        
        first_court = court_data[0]
        self.assertIn("nama_pengadilan", first_court, "Court name should be present")
        self.assertIn("link_pengadilan", first_court, "Court link should be present")
        self.assertIn("pengadilan_tinggi", first_court, "High court info should be present")
        self.assertIn("jumlah_putusan", first_court, "Decision count should be present")
        
        print(f"Successfully extracted {len(court_data)} courts.")
        print(f"First court: {first_court['nama_pengadilan']}")
        print("Assertions passed.")

    def test_get_court_list(self):
        target_url = "https://putusan3.mahkamahagung.go.id/pengadilan/index/ditjen/umum.html"
        print(f"\nTesting get_court_list method with URL: {target_url}")
        
        courts = self.scraper.get_court_list(url=target_url, max_pages=1)
        
        self.assertIsNotNone(courts, "Court list should not be None")
        self.assertIsInstance(courts, list, "Court list should be a list")
        self.assertGreater(len(courts), 0, "Court list should not be empty")
        
        if courts:
            court = courts[0]
            self.assertIn("nama_pengadilan", court, "Court name should be present")
            self.assertIn("link_pengadilan", court, "Court link should be present")
            self.assertIn("pengadilan_tinggi", court, "High court info should be present")
            self.assertIn("jumlah_putusan", court, "Decision count should be present")
            
            print(f"Successfully fetched {len(courts)} courts using get_court_list()")
            print(f"Sample court: {court['nama_pengadilan']}")
        
        print("get_court_list() test passed.")

if __name__ == '__main__':
    print("Running live URL last page extraction test...")
    unittest.main(argv=['first-arg-is-ignored'], exit=False, verbosity=2)
