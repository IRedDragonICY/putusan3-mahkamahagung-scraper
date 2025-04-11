import json
import os
import time
import requests
import re
from bs4 import BeautifulSoup, Tag
from rich.console import Console
from rich.progress import (
    Progress, BarColumn, TextColumn, TimeRemainingColumn,
    TimeElapsedColumn, TaskProgressColumn, MofNCompleteColumn,
)

class MahkamahAgungScraper:
    DEFAULT_BASE_URL = "https://putusan3.mahkamahagung.go.id/pengadilan.html"
    DEFAULT_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    def __init__(self, base_url=DEFAULT_BASE_URL, params=None, headers=None,
                 state_file="scrape_state.json", output_file="mahkamah_agung_courts.json",
                 timeout=60, retry_delay=5):
        self.base_url = base_url
        self.params = params or {}
        self.headers = headers or self.DEFAULT_HEADERS.copy()
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
        if not os.path.exists(self.state_file):
            return 1, []

        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
                self.console.log(f"[yellow]Resuming from page {state.get('next_page', 1)}...")
                return max(1, state.get('next_page', 1)), state.get('scraped_data', [])
        except Exception as e:
            self.console.log(f"[red]Error loading state: {e}. Starting fresh.")
            try:
                os.remove(self.state_file)
            except OSError:
                pass
            return 1, []

    def _save_state(self, page_to_save, data_to_save):
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump({"next_page": page_to_save, "scraped_data": data_to_save},
                          f, indent=4, ensure_ascii=False)
        except IOError as e:
            self.console.log(f"[red]Warning: Could not save state: {e}")

    def _fetch_page(self, page_number, url=None):
        params = {**self.params, 'page': page_number}
        target_url = url or self.base_url
        attempt = 0

        while True:
            attempt += 1
            try:
                response = self.session.get(target_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                self.console.log(f"[yellow]Error fetching page {page_number}, attempt {attempt}: {e}. Retrying in {self.retry_delay}s...")
                time.sleep(self.retry_delay)

    @staticmethod
    def _get_last_page(html_content):
        if not html_content: return None
        return max(1, max([int(n) for n in re.findall(r'data-ci-pagination-page="(\d+)"', html_content)], default=0))

    def _parse_data(self, html_content, current_page_num):
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', class_='table-responsive table-striped')
        if not table or not (tbody := table.find('tbody')):
            self.console.log(f"[yellow]Table data not found on page {current_page_num}.")
            return []

        page_data = []
        for row in tbody.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 4:
                continue

            try:
                nama_cell = cells[0]
                tinggi_cell = cells[1]

                nama_link = nama_cell.find('a')
                tinggi_link = tinggi_cell.find('a')

                jumlah_raw = cells[3].text.strip()
                numbers_match = re.search(r'(\d[\d.]*)(?:\s*/\s*(\d[\d.]*))?', jumlah_raw)
                putusan = int(numbers_match.group(1).replace('.', '').replace(',','')) if numbers_match and numbers_match.group(1) else None
                publikasi = int(numbers_match.group(2).replace('.', '').replace(',','')) if numbers_match and numbers_match.group(2) else None

                page_data.append({
                    "nama_pengadilan": nama_link.text.strip() if nama_link else nama_cell.text.strip(),
                    "link_pengadilan": nama_link.get('href') if nama_link else None,
                    "pengadilan_tinggi": tinggi_link.text.strip() if tinggi_link else tinggi_cell.text.strip(),
                    "link_pengadilan_tinggi": tinggi_link.get('href') if tinggi_link else None,
                    "provinsi": cells[2].text.strip(),
                    "jumlah_raw": jumlah_raw,
                    "jumlah_putusan": putusan,
                    "jumlah_publikasi": publikasi,
                })
            except Exception as e:
                self.console.log(f"[red]Error parsing row on page {current_page_num}: {e}")

        return page_data

    def get_court_list(self, url=None, start_page=1, max_pages=None, save_output=False, output_file=None):
        url = url or self.base_url
        data = []

        html = self._fetch_page(start_page, url)
        last_page = self._get_last_page(html) or 1
        end_page = min(start_page + max_pages - 1, last_page) if max_pages else last_page

        self.console.log(f"[cyan]Fetching {end_page - start_page + 1} pages from {url}")

        with Progress(BarColumn(), MofNCompleteColumn(), console=self.console) as progress:
            task = progress.add_task("", total=end_page - start_page + 1)

            for page in range(start_page, end_page + 1):
                page_html = html if page == start_page else self._fetch_page(page, url)
                data.extend(self._parse_data(page_html, page))
                progress.update(task, advance=1)

        if save_output and data:
            with open(output_file or self.output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            self.console.log(f"[green]Saved {len(data)} records")

        return data

    def scrape_list_court(self):
        self.current_page, self.all_scraped_data = self._load_state()
        start_page = self.current_page

        initial_html = self._fetch_page(1 if start_page == 1 else start_page)
        if not initial_html:
            self.console.log("[bold red]Fatal: Could not fetch initial page. Exiting.")
            return

        self.last_page = self._get_last_page(initial_html)
        if not self.last_page:
            self.console.log("[bold red]Fatal: Could not determine total pages. Exiting.")
            return

        self.console.log(f"Total pages: {self.last_page}")

        if start_page == 1 and not self.all_scraped_data:
            page_1_data = self._parse_data(initial_html, 1)
            self.all_scraped_data.extend(page_1_data)
            self._save_state(2, self.all_scraped_data)
            start_page = self.current_page = 2

        if start_page > self.last_page:
            self.console.log("[green]Scraping already complete based on saved state.")
        else:
            custom_progress = Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(), TaskProgressColumn(), MofNCompleteColumn(),
                TextColumn("[cyan]{task.fields[speed]:.2f} rec/s"), "•",
                TimeRemainingColumn(), "•", TimeElapsedColumn(),
                console=self.console, transient=False
            )

            with custom_progress as progress:
                task_id = progress.add_task(
                    "[cyan]Processing Pages...",
                    total=self.last_page,
                    completed=start_page - 1,
                    speed=0.0
                )

                total_records = 0
                run_start_time = time.monotonic()

                for page_num in range(start_page, self.last_page + 1):
                    page_start_time = time.monotonic()
                    html = initial_html if page_num == start_page else self._fetch_page(page_num)
                    initial_html = None

                    if not html:
                        self.console.log(f"[red]Failed to fetch page {page_num}. Stopping.")
                        break

                    page_data = self._parse_data(html, page_num)
                    self.all_scraped_data.extend(page_data)
                    self._save_state(page_num + 1, self.all_scraped_data)

                    duration = time.monotonic() - page_start_time
                    total_records += len(page_data)
                    self.console.log(f"Page {page_num} processed in {duration:.2f}s ({len(page_data)} records)")

                    run_elapsed = time.monotonic() - run_start_time + 1e-9
                    progress.update(task_id, advance=1, speed=total_records/run_elapsed)
                    self.current_page = page_num + 1

        is_complete = self.current_page > self.last_page
        if is_complete:
            self.console.log(f"[bold green]Scraping finished. Total records: {len(self.all_scraped_data)}")
            try:
                os.remove(self.state_file)
            except OSError:
                pass
        else:
            self.console.log(f"[yellow]Scraping interrupted at page {self.current_page-1}. Records: {len(self.all_scraped_data)}")

        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(self.all_scraped_data, f, ensure_ascii=False, indent=4)
            self.console.log(f"[bold green]Data saved to {self.output_file}")
        except Exception as e:
            self.console.log(f"[red]Error saving data: {e}")

    def scrape(self):
        self.scrape_list_court()

    def get_court_yearly_decisions(self, court_code=None, url=None):
        if not url and not court_code:
            raise ValueError("Either court_code or url must be provided")

        if not url:
            url = f"https://putusan3.mahkamahagung.go.id/direktori/periode/tahunjenis/putus/pengadilan/{court_code}.html"

        self.console.log(f"[cyan]Fetching yearly decision data from: {url}")

        html = self._fetch_page(1, url)
        if not html:
            self.console.log("[red]Failed to fetch court yearly decisions page")
            return []

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table', class_='table-striped')

        if not table or not (tbody := table.find('tbody')):
            self.console.log("[yellow]No yearly decision table found on page")
            return []

        yearly_data = []

        for row in tbody.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 2:
                continue

            try:
                year_cell = cells[0]
                count_cell = cells[1]

                year_link = year_cell.find('a')
                count_link = count_cell.find('a')

                if not year_link or not count_link:
                    continue

                year = year_link.text.strip()
                count_raw = count_link.text.strip().replace('.', '').replace(',', '')
                count = int(count_raw) if count_raw.isdigit() else 0
                link = year_link.get('href')

                yearly_data.append({
                    "year": year,
                    "decision_count": count,
                    "link": link
                })
            except Exception as e:
                self.console.log(f"[red]Error parsing yearly decision row: {e}")

        self.console.log(f"[green]Successfully extracted {len(yearly_data)} yearly decision records")
        return yearly_data

    def get_court_decision_categories_by_year(self, url):
        if not url:
            raise ValueError("URL must be provided")

        self.console.log(f"[cyan]Fetching decision categories from: {url}")
        html = self._fetch_page(1, url)
        if not html:
            self.console.log("[red]Failed to fetch decision category page")
            return []

        soup = BeautifulSoup(html, 'html.parser')
        category_data = []

        container = soup.find('div', id='collapseZero')
        if not container:
            self.console.log("[yellow]Category container ('#collapseZero') not found.")
            return []

        form_checks = container.find_all('div', class_='form-check')
        if not form_checks:
             self.console.log("[yellow]No 'form-check' elements found within container.")
             return []

        for check in form_checks:
            p_tag = check.find('p', class_='card-text')
            if not p_tag: continue
            a_tag = p_tag.find('a')
            if not a_tag: continue

            link = a_tag.get('href')
            span_tag = a_tag.find('span', class_=re.compile(r'\bbadge\b'))

            category_name_parts = []
            for content in a_tag.contents:
                 if isinstance(content, str):
                     stripped_content = content.strip()
                     if stripped_content:
                         category_name_parts.append(stripped_content)
                 elif isinstance(content, Tag) and content.name != 'span':
                     stripped_content = content.text.strip()
                     if stripped_content:
                          category_name_parts.append(stripped_content)

            category_name = " ".join(category_name_parts).strip()

            if category_name == "Semua Direktori":
                continue

            count = 0
            if span_tag:
                count_raw = span_tag.text.strip().replace('.', '').replace(',', '')
                if count_raw.isdigit():
                    count = int(count_raw)
                else:
                     self.console.log(f"[yellow]Could not parse count '{span_tag.text.strip()}' for category '{category_name}'")
            else:
                 self.console.log(f"[yellow]Count span not found for category '{category_name}'")


            if category_name and link:
                 category_data.append({
                    "category": category_name,
                    "count": count,
                    "link": link
                 })
            else:
                 self.console.log(f"[yellow]Skipping entry due to missing name or link: {a_tag.prettify()}")


        self.console.log(f"[green]Successfully extracted {len(category_data)} decision category records")
        return category_data