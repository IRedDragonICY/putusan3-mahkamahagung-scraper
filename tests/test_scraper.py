import json
import os
import sys
import unittest
import requests
from urllib.parse import urlparse

from MahkamahAgungScraper import MahkamahAgungScraper


class TestMahkamahAgungScraperLive(unittest.TestCase):

    def setUp(self):
        self.scraper = MahkamahAgungScraper()
        self.maxDiff = None
        self.output_dir = "output_data"  # Define output directory
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def _download_pdf(self, url):
        """Helper function to download and save a PDF."""
        if not url:
            self.fail("PDF URL is None, cannot download.")

        try:
            response = requests.get(url, stream=True, timeout=30)  # Timeout added
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

            # Extract filename from URL
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            filepath = os.path.join(self.output_dir, filename)

            with open(filepath, 'wb') as pdf_file:
                for chunk in response.iter_content(chunk_size=8192):  # 8KB chunks
                    pdf_file.write(chunk)

            print(f"PDF downloaded successfully to: {filepath}")
            return filepath  # Return filepath for assertion purposes

        except requests.exceptions.RequestException as e:
            self.fail(f"Failed to download PDF from {url}: {e}")
            return None

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
        last_page = self.scraper.get_last_page(live_html)
        print(f"[Test: Last Page - List] Parsed last page: {last_page}")
        self.assertIsInstance(last_page, int)
        self.assertGreaterEqual(last_page, 1)
        print("[Test: Last Page - List] Assertions passed.")

    def test_extract_last_page_from_one_of_pengadilan(self):
        target_url = "https://putusan3.mahkamahagung.go.id/direktori/index/pengadilan/pn-airmadidi"
        print(f"\n[Test: Last Page - Detail] Fetching live data from: {target_url}")
        live_html = None
        try:
            live_html = self.scraper._fetch_page(1, target_url)
        except Exception as e:
            self.fail(f"Failed to fetch live HTML from {target_url}: {e}")
        self.assertIsNotNone(live_html, f"Received None HTML from {target_url}")
        print("[Test: Last Page - Detail] Live HTML fetched. Parsing last page...")
        last_page = self.scraper.get_last_page(live_html)
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
        court_data = self.scraper.get_list_courts(url=target_url)
        self.assertIsNotNone(court_data)
        self.assertIsInstance(court_data, list)
        print(f"[Test: Court List Parse] Found {len(court_data)} courts on page 1.")
        self.assertGreater(len(court_data), 0)
        if court_data:
            first_court = court_data[0]
            print(
                f"[Test: Court List Parse] Verifying structure of first court: {first_court.get('nama_pengadilan', 'N/A')}")
            self.assertIn("nama_pengadilan", first_court)
            self.assertIsInstance(first_court["nama_pengadilan"], str)
            self.assertIn("link_pengadilan", first_court)
            self.assertTrue(isinstance(first_court["link_pengadilan"], str) or first_court["link_pengadilan"] is None)
            self.assertIn("pengadilan_tinggi", first_court)
            self.assertIsInstance(first_court["pengadilan_tinggi"], str)
            self.assertIn("link_pengadilan_tinggi", first_court)
            self.assertTrue(
                isinstance(first_court["link_pengadilan_tinggi"], str) or first_court["link_pengadilan_tinggi"] is None)
            self.assertIn("provinsi", first_court)
            self.assertIsInstance(first_court["provinsi"], str)
            self.assertIn("jumlah_putusan", first_court)
            self.assertTrue(isinstance(first_court["jumlah_putusan"], int) or first_court["jumlah_putusan"] is None)
            self.assertIn("jumlah_publikasi", first_court)
            self.assertTrue(isinstance(first_court["jumlah_publikasi"], int) or first_court["jumlah_publikasi"] is None)
        print("[Test: Court List Parse] Assertions passed.")

    def test_get_court_list(self):
        target_url = "https://putusan3.mahkamahagung.go.id/pengadilan/index/ditjen/umum.html"
        print(f"\n[Test: Get Court List Method] Testing get_list_courts with URL: {target_url}")
        courts = self.scraper.get_list_courts(url=target_url)
        self.assertIsNotNone(courts)
        self.assertIsInstance(courts, list)
        print(f"[Test: Get Court List Method] Fetched {len(courts)} courts using get_list_courts().")
        self.assertGreater(len(courts), 0)

        # Print some data (limited to first 5 courts)
        print("\n[Test: Get Court List Method] Displaying data for the first 5 courts:")
        for i, court in enumerate(courts[:5]):
            print(f"  Court #{i + 1}:")
            print(f"    Nama Pengadilan: {court.get('nama_pengadilan', 'N/A')}")
            print(f"    Link Pengadilan: {court.get('link_pengadilan', 'N/A')}")
            print(f"    Provinsi: {court.get('provinsi', 'N/A')}")
            print(f"    Jumlah Putusan: {court.get('jumlah_putusan', 'N/A')}")
            print("-" * 30)

        if courts:
            court = courts[0]
            print(f"[Test: Get Court List Method] Sample court: {court.get('nama_pengadilan', 'N/A')}")
            self.assertIn("nama_pengadilan", court)
            self.assertIn("link_pengadilan", court)
            self.assertIn("pengadilan_tinggi", court)
            self.assertIn("provinsi", court)
            self.assertIn("jumlah_putusan", court)
        print("[Test: Get Court List Method] Test passed.")

    def test_get_court_list_all_pages(self):
        target_url = "https://putusan3.mahkamahagung.go.id/pengadilan/index/ditjen/umum.html"
        print(
            f"\n[Test: Get All Court List Method] Testing get_list_courts to fetch all pages from: {target_url}")
        first_page_html = self.scraper._fetch_page(1, target_url)
        self.assertIsNotNone(first_page_html, f"Failed to fetch first page HTML from {target_url}")
        last_page = self.scraper.get_last_page(first_page_html)
        self.assertIsInstance(last_page, int)
        print(f"[Test: Get All Court List Method] Last page is {last_page}")

        all_courts = []
        for page in range(1, last_page + 1):
            print(f"[Test: Get All Court List Method] Fetching data from page {page}/{last_page}")
            try:
                courts_on_page = self.scraper.get_list_courts(url=f"{target_url}?page={page}")
                self.assertIsNotNone(courts_on_page)
                self.assertIsInstance(courts_on_page, list)
                print(f"[Test: Get All Court List Method] Fetched {len(courts_on_page)} courts from page {page}")
                all_courts.extend(courts_on_page)
            except Exception as e:
                self.fail(f"[Test: Get All Court List Method] Failed to fetch court list from page {page}: {e}")

        print(f"[Test: Get All Court List Method] Total courts fetched from all pages: {len(all_courts)}")
        self.assertGreater(len(all_courts), 0)

        # Basic Structure check
        if all_courts:
            court = all_courts[0]
            self.assertIn("nama_pengadilan", court)
            self.assertIn("link_pengadilan", court)
            self.assertIn("pengadilan_tinggi", court)
            self.assertIn("provinsi", court)
            self.assertIn("jumlah_putusan", court)

            # Print some data (limited to first 5 courts)
            print("\n[Test: Get All Court List Method] Displaying data for the first 5 courts:")
            for i, court in enumerate(all_courts[:5]):
                print(f"  Court #{i + 1}:")
                print(f"    Nama Pengadilan: {court.get('nama_pengadilan', 'N/A')}")
                print(f"    Link Pengadilan: {court.get('link_pengadilan', 'N/A')}")
                print(f"    Provinsi: {court.get('provinsi', 'N/A')}")
                print(f"    Jumlah Putusan: {court.get('jumlah_putusan', 'N/A')}")
                print("-" * 30)

        print("[Test: Get All Court List Method] Test passed.")

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
        self.assertIsNotNone(yearly_data_url)
        self.assertIsInstance(yearly_data_url, list)
        print(f"[Test: Yearly Decisions] Fetched {len(yearly_data_url)} yearly records via URL.")
        self.assertGreater(len(yearly_data_url), 0)
        if yearly_data_url:
            print("[Test: Yearly Decisions] All results (URL):")
            for i, record in enumerate(yearly_data_url):
                print(
                    f"  Record #{i + 1}: Year={record.get('year', 'N/A')}, Count={record.get('decision_count', 'N/A')}, Link={record.get('link', 'N/A')}")

        print(f"[Test: Yearly Decisions] Attempt 2: Fetching via court_code: {test_court_code} (for comparison)")
        yearly_data_code = []
        try:
            yearly_data_code = self.scraper.get_court_yearly_decisions(court_code=test_court_code)
        except Exception as e:
            self.fail(f"[Test: Yearly Decisions] Failed to fetch yearly decision data via court_code: {e}")
        self.assertIsNotNone(yearly_data_code)
        self.assertIsInstance(yearly_data_code, list)
        print(f"[Test: Yearly Decisions] Fetched {len(yearly_data_code)} yearly records via court_code.")
        print("[Test: Yearly Decisions] Comparing results from URL and court_code methods...")
        self.assertEqual(len(yearly_data_url), len(yearly_data_code))
        self.assertListEqual(yearly_data_url, yearly_data_code)
        print("[Test: Yearly Decisions] Assertions and comparison passed.")

    def test_get_court_decision_categories_by_year(self):
        target_url = "https://putusan3.mahkamahagung.go.id/direktori/index/pengadilan/pn-airmadidi/tahunjenis/putus/tahun/2025.html"
        print(f"\n[Test: Decision Categories] Fetching categories from: {target_url}")
        categories = []
        try:
            categories = self.scraper.get_court_decision_categories_by_year(url=target_url)
        except Exception as e:
            self.fail(f"[Test: Decision Categories] Failed to fetch category data: {e}")
        self.assertIsNotNone(categories)
        self.assertIsInstance(categories, list)
        print(f"[Test: Decision Categories] Fetched {len(categories)} category records.")
        self.assertGreater(len(categories), 0)
        print("[Test: Decision Categories] Sample fetched categories:")
        for i, item in enumerate(categories):
            print(
                f"  Record {i + 1}: Category='{item.get('category', 'N/A')}', Count={item.get('count', 'N/A')}', Link='{item.get('link', 'N/A')}'")
        has_semua_direktori = any(item.get("category") == "Semua Direktori" for item in categories)
        self.assertFalse(has_semua_direktori, "[Test: Decision Categories] 'Semua Direktori' should be excluded.")
        print("[Test: Decision Categories] Assertions passed.")

    def test_get_decision_classifications(self):
        target_url = "https://putusan3.mahkamahagung.go.id/direktori/index/pengadilan/pn-airmadidi/kategori/perdata-1/tahunjenis/putus/tahun/2025.html"
        print(f"\n[Test: Decision Classifications] Fetching classifications from: {target_url}")
        classifications = []
        try:
            classifications = self.scraper.get_decision_classifications(url=target_url)
        except Exception as e:
            self.fail(f"[Test: Decision Classifications] Failed to fetch classification data: {e}")
        self.assertIsNotNone(classifications)
        self.assertIsInstance(classifications, list)
        print(f"[Test: Decision Classifications] Fetched {len(classifications)} classification records.")
        self.assertGreater(len(classifications), 0)
        print("[Test: Decision Classifications] Sample fetched classifications:")
        parent_category_name = "Perdata"
        includes_parent_category = False
        for i, item in enumerate(classifications):
            print(
                f"  Record {i + 1}: Classification='{item.get('classification', 'N/A')}', Count={item.get('count', 'N/A')}', Link='{item.get('link', 'N/A')}'")
            if item.get("classification") == parent_category_name:
                includes_parent_category = True
        if not includes_parent_category:
            includes_parent_category = any(item.get("classification") == parent_category_name for item in
                                            classifications)
        self.assertTrue(includes_parent_category,
                        f"[Test: Decision Classifications] Expected parent category '{parent_category_name}' not found.")
        print("[Test: Decision Classifications] Assertions passed.")

    def test_get_monthly_decision_counts(self):
        target_url = "https://putusan3.mahkamahagung.go.id/direktori/index/pengadilan/pn-airmadidi/kategori/perdata-1/tahunjenis/putus/tahun/2025.html"
        print(f"\n[Test: Monthly Counts] Fetching monthly counts from: {target_url}")
        monthly_counts = []
        try:
            monthly_counts = self.scraper.get_monthly_decision_counts(url=target_url)
        except Exception as e:
            self.fail(f"[Test: Monthly Counts] Failed to fetch monthly count data: {e}")
        self.assertIsNotNone(monthly_counts)
        self.assertIsInstance(monthly_counts, list)
        print(f"[Test: Monthly Counts] Fetched {len(monthly_counts)} monthly count records.")
        self.assertGreater(len(monthly_counts), 0)
        print("[Test: Monthly Counts] Sample fetched monthly counts:")
        for i, item in enumerate(monthly_counts):
            print(f"  Record {i + 1}: Month='{item.get('month', 'N/A')}', Count={item.get('count', 'N/A')}")
        expected_months = ["Januari", "Februari", "Maret", "April", "Mei", "Juni", "Juli", "Agustus", "September",
                           "Oktober", "November", "Desember"]
        valid_months = all(item.get("month") in expected_months for item in monthly_counts)
        self.assertTrue(valid_months, "[Test: Monthly Counts] Found invalid month names.")
        print("[Test: Monthly Counts] Assertions passed.")

    def test_get_decision_list(self):
        target_url = "https://putusan3.mahkamahagung.go.id/direktori/index/pengadilan/pn-airmadidi/kategori/perdata-1/tahunjenis/putus/tahun/2025.html"
        print(f"\n[Test: Decision List] Fetching decision list from: {target_url}")
        decision_list = []
        try:
            decision_list = self.scraper.get_decision_list(url=target_url)
        except Exception as e:
            self.fail(f"[Test: Decision List] Failed to fetch decision list data: {e}")
        self.assertIsNotNone(decision_list)
        self.assertIsInstance(decision_list, list)
        print(f"[Test: Decision List] Fetched {len(decision_list)} decision entries.")
        self.assertGreater(len(decision_list), 0)
        print("[Test: Decision List] Displaying details of the first 2 fetched decisions:")
        for i, item in enumerate(decision_list[:2]):
            print(f"--- Decision {i + 1} ---")
            print(json.dumps(item, indent=2, ensure_ascii=False))
            self.assertIsInstance(item, dict)
            if i == 0:
                self.assertIn("breadcrumbs", item)
                self.assertIsInstance(item["breadcrumbs"], list)
                self.assertIn("register_date", item)
                self.assertTrue(isinstance(item["register_date"], str) or item["register_date"] is None)
                if isinstance(item["register_date"], str):
                    self.assertRegex(item["register_date"], r"\d{2}-\d{2}-\d{4}")
                self.assertIn("putus_date", item)
                self.assertTrue(isinstance(item["putus_date"], str) or item["putus_date"] is None)
                if isinstance(item["putus_date"], str):
                    self.assertRegex(item["putus_date"], r"\d{2}-\d{2}-\d{4}")
                self.assertIn("upload_date", item)
                self.assertTrue(isinstance(item["upload_date"], str) or item["upload_date"] is None)
                if isinstance(item["upload_date"], str):
                    self.assertRegex(item["upload_date"], r"\d{2}-\d{2}-\d{4}")
                self.assertIn("title", item)
                self.assertTrue(isinstance(item["title"], str) or item["title"] is None)
                self.assertIn("link", item)
                self.assertTrue(isinstance(item["link"], str) or item["link"] is None)
                if isinstance(item["link"], str):
                    self.assertTrue(item["link"].startswith("http"))
                self.assertIn("description_parties", item)
                self.assertIsInstance(item["description_parties"], str)
                self.assertIn("view_count", item)
                self.assertIsInstance(item["view_count"], int)
                self.assertGreaterEqual(item["view_count"], 0)
                self.assertIn("download_count", item)
                self.assertIsInstance(item["download_count"], int)
                self.assertGreaterEqual(item["download_count"], 0)
        print("[Test: Decision List] Basic structure assertions passed for first item.")

    def test_get_decision_detail_and_download_pdf(self):
        target_url = "https://putusan3.mahkamahagung.go.id/direktori/putusan/zaf01517944fefa48152313435323236.html"
        print(f"\n[Test: Decision Detail and PDF Download] Fetching details from: {target_url}")
        details = None
        try:
            details = self.scraper.get_decision_detail(url=target_url)
        except Exception as e:
            self.fail(f"[Test: Decision Detail and PDF Download] Failed to fetch/parse decision detail: {e}")

        self.assertIsNotNone(details, "[Test: Decision Detail] Result dictionary should not be None")
        self.assertIsInstance(details, dict, "[Test: Decision Detail] Result should be a dictionary")
        self.assertGreater(len(details), 5,
                           "[Test: Decision Detail] Result dictionary should have several keys")

        print("[Test: Decision Detail] Displaying fetched details:")
        print(json.dumps(details, indent=2, ensure_ascii=False, default=str))

        self.assertIn("nomor", details)
        self.assertIsInstance(details.get("nomor"), str)
        self.assertEqual(details.get("nomor"), "3/Pdt.G.S/2025/PN Arm")

        self.assertIn("tahun", details)
        self.assertIsInstance(details.get("tahun"), str)
        self.assertEqual(details.get("tahun"), "2025")

        self.assertIn("klasifikasi", details)
        self.assertIsInstance(details.get("klasifikasi"), list)
        self.assertIn("Perdata", details.get("klasifikasi", []))

        self.assertIn("catatan_amar", details)
        self.assertIsInstance(details.get("catatan_amar"), str)
        self.assertTrue(details.get("catatan_amar", "").strip() != "")

        self.assertIn("download_link_pdf", details)
        self.assertTrue(isinstance(details.get("download_link_pdf"), str) or details.get("download_link_pdf") is None)
        pdf_url = details.get("download_link_pdf")

        if pdf_url:
            print(f"[Test: PDF Download] Attempting to download PDF from: {pdf_url}")
            filepath = self._download_pdf(pdf_url)  # Call helper function

            self.assertIsNotNone(filepath, "Filepath should not be None after attempted download") # Filepath check
            if filepath:
                self.assertTrue(os.path.exists(filepath), "Downloaded PDF file does not exist on disk") # Existence check
                self.assertTrue(os.path.isfile(filepath), "Downloaded file is not a file.") # File type
                self.assertGreater(os.path.getsize(filepath), 1000, "Downloaded PDF is too small, likely incomplete.")  # Size check (1KB minimum)
        else:
            print("[Test: PDF Download] No PDF download link found in details. Skipping PDF download test.")

        self.assertIn("download_link_zip", details)
        self.assertTrue(isinstance(details.get("download_link_zip"), str) or details.get("download_link_zip") is None)
        if details.get("download_link_zip"):
            self.assertIn("/zip/", details["download_link_zip"], "ZIP download link should contain '/zip/'")
            self.assertTrue(details["download_link_zip"].startswith("http"), "PDF download link should be a full URL")

        self.assertIn("title_full", details)
        self.assertTrue(isinstance(details.get("title_full"), str) or details.get("title_full") is None,
                        "Title should be string or None")
        if details.get("title_full") is not None:
            self.assertIn("Putusan PN AIRMADIDI", details["title_full"])

        print("[Test: Decision Detail] Assertions passed.")


if __name__ == '__main__':
    print("Running Mahkamah Agung Scraper Live Tests...")
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestMahkamahAgungScraperLive))
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    runner.run(suite)