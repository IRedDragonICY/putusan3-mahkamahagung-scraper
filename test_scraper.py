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


if __name__ == '__main__':
    print("Running live URL last page extraction test...")
    unittest.main(argv=['first-arg-is-ignored'], exit=False, verbosity=2)
