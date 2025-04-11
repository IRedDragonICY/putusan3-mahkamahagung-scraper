import json
import os
import time
import requests
import re
from bs4 import BeautifulSoup
from rich.console import Console
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    TimeElapsedColumn,
    TaskProgressColumn,
    MofNCompleteColumn,
)

class MahkamahAgungScraper:
    DEFAULT_BASE_URL = "https://putusan3.mahkamahagung.go.id/pengadilan.html"
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    DEFAULT_STATE_FILE = "scrape_state.json"
    DEFAULT_OUTPUT_FILE = "mahkamah_agung_courts.json"
    DEFAULT_REQUEST_TIMEOUT = 60
    DEFAULT_RETRY_DELAY = 5

    def __init__(self,
                 base_url=DEFAULT_BASE_URL,
                 params=None,
                 headers=None,
                 state_file=DEFAULT_STATE_FILE,
                 output_file=DEFAULT_OUTPUT_FILE,
                 timeout=DEFAULT_REQUEST_TIMEOUT,
                 retry_delay=DEFAULT_RETRY_DELAY):
        self.base_url = base_url
        self.params = params if params is not None else {}
        self.headers = headers if headers is not None else self.DEFAULT_HEADERS.copy()
        self.state_file = state_file
        self.output_file = output_file
        self.timeout = timeout
        self.retry_delay = retry_delay
        self.console = Console()
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.current_page = 1
        self.all_scraped_data = []
        self.last_page = None

    def _load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    next_page = max(1, state.get('next_page', 1))
                    scraped_data = state.get('scraped_data', [])
                    self.console.log(f"[yellow]Resuming from page {next_page}...")
                    return next_page, scraped_data
            except (json.JSONDecodeError, IOError, KeyError) as e:
                self.console.log(f"[red]Error loading state file '{self.state_file}': {e}. Starting from scratch.")
                try:
                    os.remove(self.state_file)
                    self.console.log(f"[yellow]Removed corrupted state file '{self.state_file}'.")
                except OSError:
                     pass
                return 1, []
        return 1, []

    def _save_state(self, page_to_save, data_to_save):
        state = {
            "next_page": page_to_save,
            "scraped_data": data_to_save
        }
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=4, ensure_ascii=False)
        except IOError as e:
            self.console.log(f"[red]Warning: Could not save state to '{self.state_file}': {e}")

    def _fetch_page(self, page_number, url=None):
        params = self.params.copy()
        params['page'] = page_number
        target_url = url if url else self.base_url
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self.session.get(target_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.text
            except requests.exceptions.Timeout:
                self.console.log(
                    f"[yellow]Timeout fetching page {page_number}, attempt {attempt}. Retrying in {self.retry_delay}s...")
            except requests.exceptions.RequestException as e:
                self.console.log(
                    f"[red]Error fetching page {page_number}, attempt {attempt}: {e}. Retrying in {self.retry_delay}s...")
            time.sleep(self.retry_delay)

    @staticmethod
    def _get_last_page(html_content):
        if not html_content: return None
        try:
            page_numbers = [int(n) for n in re.findall(r'data-ci-pagination-page="(\d+)"', html_content)]
            return max(1, max(page_numbers, default=0))
        except Exception:
            return None

    def _parse_data(self, html_content, current_page_num):
        if not html_content:
            return []
        page_data = []
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup.find('table', class_='table-responsive table-striped')
            if not table:
                self.console.log(f"[yellow]Data table not found on page {current_page_num}.")
                return []
            tbody = table.find('tbody')
            if not tbody:
                self.console.log(f"[yellow]Table body (tbody) not found on page {current_page_num}.")
                return []
            rows = tbody.find_all('tr')
            for i, row in enumerate(rows):
                cells = row.find_all('td')
                if len(cells) >= 4:
                    try:
                        nama_link_tag = cells[0].find('a')
                        nama_pengadilan = nama_link_tag.text.strip() if nama_link_tag else cells[0].text.strip()
                        link_pengadilan = nama_link_tag['href'] if nama_link_tag and 'href' in nama_link_tag.attrs else None

                        tinggi_link_tag = cells[1].find('a')
                        pengadilan_tinggi = tinggi_link_tag.text.strip() if tinggi_link_tag else cells[1].text.strip()
                        link_pengadilan_tinggi = tinggi_link_tag['href'] if tinggi_link_tag and 'href' in tinggi_link_tag.attrs else None

                        provinsi = cells[2].text.strip()
                        jumlah_raw = cells[3].text.strip()
                        jumlah_putusan = None
                        jumlah_publikasi = None

                        if '/' in jumlah_raw:
                            parts = [p.strip().replace('.', '') for p in jumlah_raw.split('/', 1)]
                            if len(parts) == 2:
                                try:
                                    jumlah_putusan = int(parts[0]) if parts[0].isdigit() else None
                                except ValueError:
                                    jumlah_putusan = None
                                try:
                                    jumlah_publikasi = int(parts[1]) if parts[1].isdigit() else None
                                except ValueError:
                                    jumlah_publikasi = None
                        else:
                             cleaned_raw = jumlah_raw.replace('.', '')
                             if cleaned_raw.isdigit():
                                 jumlah_putusan = int(cleaned_raw)

                        page_data.append({
                            "nama_pengadilan": nama_pengadilan,
                            "link_pengadilan": link_pengadilan,
                            "pengadilan_tinggi": pengadilan_tinggi,
                            "link_pengadilan_tinggi": link_pengadilan_tinggi,
                            "provinsi": provinsi,
                            "jumlah_raw": jumlah_raw,
                            "jumlah_putusan": jumlah_putusan,
                            "jumlah_publikasi": jumlah_publikasi,
                        })
                    except Exception as cell_error:
                        self.console.log(f"[yellow]Skipping row {i+1} on page {current_page_num} due to parsing error: {cell_error}. Row content: {row.text[:100]}...")
                else:
                    self.console.log(f"[yellow]Skipping row {i+1} on page {current_page_num} due to insufficient cells ({len(cells)} found).")
        except Exception as e:
            self.console.log(f"[red]Error parsing table data on page {current_page_num}: {e}")
        return page_data

    def scrape(self):
        self.current_page, self.all_scraped_data = self._load_state()
        start_page = self.current_page

        if start_page > 1:
            self.console.log(f"Attempting to fetch resume page {start_page} to re-check total pages...")
            initial_html = self._fetch_page(start_page)
            if initial_html:
                self.last_page = self._get_last_page(initial_html)
                self.console.log(f"Total pages re-checked: {self.last_page if self.last_page else 'Could not determine'}")
            else:
                self.console.log(f"[bold red]Fatal: Could not fetch resume page {start_page}. Exiting.")
                return
        else:
            self.console.log("Starting fresh. Fetching page 1 to determine total pages...")
            initial_html = self._fetch_page(1)
            if initial_html:
                self.last_page = self._get_last_page(initial_html)
                if self.last_page is not None and not self.all_scraped_data:
                    page_1_data = self._parse_data(initial_html, 1)
                    self.all_scraped_data.extend(page_1_data)
                    self.console.log(f"Page 1 processed initially ({len(page_1_data)} records).")
                    self._save_state(2, self.all_scraped_data)
                    start_page = 2
                    self.current_page = 2
                elif self.last_page is None:
                     self.console.log("[bold red]Fatal: Could not determine total pages from page 1. Exiting.")
                     return
            else:
                self.console.log("[bold red]Fatal: Could not fetch page 1. Exiting.")
                return

        if self.last_page is None:
            self.console.log("[bold red]Fatal: Could not determine the total number of pages after initial fetch. Exiting.")
            return

        self.console.log(f"Total pages determined: {self.last_page}")

        actual_start_page_for_loop = start_page

        if actual_start_page_for_loop > self.last_page:
            self.console.log("[green]Scraping appears to be already complete based on saved state and total pages.")
        else:
            custom_progress = Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                MofNCompleteColumn(),
                TextColumn("[cyan]{task.fields[speed]:.2f} rec/s"),
                "•",
                TimeRemainingColumn(),
                "•",
                TimeElapsedColumn(),
                console=self.console,
                transient=False
            )

            with custom_progress as progress:
                task_id = progress.add_task(
                    "[cyan]Processing Pages...",
                    total=self.last_page,
                    completed=actual_start_page_for_loop - 1,
                    speed=0.0
                )

                total_records_processed_in_run = 0
                run_start_time = time.monotonic()

                for loop_current_page in range(actual_start_page_for_loop, self.last_page + 1):
                    page_start_time = time.monotonic()

                    if loop_current_page == start_page and initial_html:
                         html = initial_html
                         self.console.log(f"Using pre-fetched content for page {loop_current_page}.")
                    else:
                        html = self._fetch_page(loop_current_page)

                    if html:
                        page_data = self._parse_data(html, loop_current_page)
                        self.all_scraped_data.extend(page_data)
                        self._save_state(loop_current_page + 1, self.all_scraped_data)
                        page_end_time = time.monotonic()
                        duration = page_end_time - page_start_time
                        total_records_processed_in_run += len(page_data)
                        self.console.log(f"Page {loop_current_page} processed in {duration:.2f}s ({len(page_data)} records). State saved.")
                    else:
                        self.console.log(f"[red]Fetch/Parse failed for page {loop_current_page}. State not saved for this page. Check logs above.")
                        break

                    run_elapsed_time = time.monotonic() - run_start_time + 1e-9
                    current_speed = total_records_processed_in_run / run_elapsed_time

                    progress.update(task_id, advance=1, speed=current_speed)
                    initial_html = None
                    self.current_page = loop_current_page + 1


        final_next_page = self.current_page

        if final_next_page > self.last_page:
             self.console.log(f"[bold green]Scraping finished. Processed up to page {self.last_page}. Total records collected: {len(self.all_scraped_data)}")
             if os.path.exists(self.state_file):
                try:
                    os.remove(self.state_file)
                    self.console.log(f"[blue]State file {self.state_file} removed as scraping completed.")
                except OSError as e:
                    self.console.log(f"[red]Warning: Could not remove state file {self.state_file}: {e}")
        else:
             self.console.log(f"[yellow]Scraping stopped or was interrupted. Next page to process is {final_next_page}. State file {self.state_file} kept for resuming. Total records collected so far: {len(self.all_scraped_data)}")


        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(self.all_scraped_data, f, ensure_ascii=False, indent=4)
            self.console.log(f"[bold green]Data successfully saved to {self.output_file}")
        except IOError as e:
            self.console.log(f"[red]Error saving final data to {self.output_file}: {e}")
        except Exception as e:
             self.console.log(f"[red]An unexpected error occurred during final data saving: {e}")


