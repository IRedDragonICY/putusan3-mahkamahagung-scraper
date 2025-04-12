import json
import os
import time
import requests
import re
from bs4 import BeautifulSoup, Tag, NavigableString
from rich.console import Console
from rich.progress import (
    Progress, BarColumn, TextColumn, TimeRemainingColumn,
    TimeElapsedColumn, TaskProgressColumn, MofNCompleteColumn,
)


def _find_card_by_header(soup, header_text):
    # Helper to find card based on header text
     for card in soup.find_all('div', class_='card'):
         header = card.find('div', class_='card-header')
         if header:
              togglet = header.find('div', class_='togglet')
              if togglet and header_text in togglet.text:
                   return card
     return None


def _extract_items_from_card(card, item_selector='div.form-check', link_selector='a', count_selector='span.badge', name_cleaner_func=None):
    # Helper to extract list items (like category, classification, month)
    if not card: return []
    items_data = []
    container = card.find('div', class_=re.compile(r'\bcollapse\b'))
    if not container: return []
    card_body = container.find('div', class_='card-body')
    if not card_body: return []
    items = card_body.find_all(item_selector, recursive=False) if item_selector != 'li' else card_body.select('ul.portfolio-meta > li')

    link_next = False # Flag for download links
    for item in items:
        # Handling download links which are in the next li
        if link_next:
            link_tag = item.find(link_selector)
            if link_tag:
                link_href = link_tag.get('href')
                items_data[-1]['link'] = link_href # Add link to the previously added item
            link_next = False
            continue

        p_tag = item.find('p', class_='card-text') if item.name != 'li' else item
        if not p_tag: continue

        link_tag = p_tag.find(link_selector)
        span_tag = p_tag.find(count_selector) if count_selector else None

        # Special handling for months (no link tag expected)
        is_month = "Bulan" in card.find('div', class_='togglet').text if card.find('div', class_='togglet') else False
        if is_month:
            full_text = p_tag.text.strip()
            item_name = full_text
            count = 0
            if span_tag:
                count_raw = span_tag.text.strip().replace('.', '').replace(',', '')
                count = int(count_raw) if count_raw.isdigit() else 0
                item_name = full_text.replace(span_tag.text, '').strip()
            if item_name: items_data.append({"month": item_name, "count": count})

        # Handling download section structure
        elif "Lampiran" in card.find('div', class_='togglet').text if card.find('div', class_='togglet') else False:
             label_span = item.find('span')
             if label_span:
                  label_text = label_span.text.strip()
                  if "Download Zip" in label_text:
                       items_data.append({"type": "zip", "link": None})
                       link_next = True
                  elif "Download PDF" in label_text:
                       items_data.append({"type": "pdf", "link": None})
                       link_next = True

        # General handling for categories/classifications
        elif link_tag:
            link = link_tag.get('href')
            name_parts = []
            for content in link_tag.contents:
                if isinstance(content, str):
                    stripped = content.strip()
                    if stripped: name_parts.append(stripped)
                elif isinstance(content, Tag) and content.name != 'span':
                    stripped = content.text.strip()
                    if stripped: name_parts.append(stripped)
            item_name = " ".join(name_parts).strip()

            if name_cleaner_func and name_cleaner_func(item_name): continue # Skip if cleaner returns True

            count = 0
            if span_tag:
                count_raw = span_tag.text.strip().replace('.', '').replace(',', '')
                count = int(count_raw) if count_raw.isdigit() else 0

            if item_name and link:
                # Determine key based on context (simple way)
                key_name = "classification" if "Klasifikasi" in card.find('div','togglet').text else "category"
                items_data.append({key_name: item_name, "count": count, "link": link})
        # Add more specific handling if needed
    return items_data


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
                if os.path.exists(self.state_file): os.remove(self.state_file)
            except OSError: pass
            return 1, []

    def _save_state(self, page_to_save, data_to_save):
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump({"next_page": page_to_save, "scraped_data": data_to_save},
                          f, indent=4, ensure_ascii=False)
        except IOError as e:
            self.console.log(f"[red]Warning: Could not save state: {e}")

    def _fetch_page(self, page_number, url=None):
        params = {**self.params}
        if url is None: params['page'] = page_number
        target_url = url or self.base_url
        attempt = 0
        while True:
            attempt += 1
            current_params = params if url is None or 'page' not in url else None
            try:
                response = self.session.get(target_url, params=current_params, timeout=self.timeout)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                self.console.log(f"[yellow]Error fetching page {page_number if url is None else ''} ({target_url}), attempt {attempt}: {e}. Retrying in {self.retry_delay}s...")
                time.sleep(self.retry_delay)

    @staticmethod
    def _get_last_page(html_content):
        if not html_content: return None
        soup = BeautifulSoup(html_content, 'html.parser')
        last_link = soup.select_one('ul.pagination li:last-child a[data-ci-pagination-page]')
        if last_link and last_link['data-ci-pagination-page'].isdigit():
            return int(last_link['data-ci-pagination-page'])
        pages = [int(a['data-ci-pagination-page']) for a in soup.select('ul.pagination a[data-ci-pagination-page]') if a['data-ci-pagination-page'].isdigit()]
        return max(pages) if pages else 1

    def _parse_data(self, html_content, current_page_num):
        # Parses court list pages
        if not html_content: return []
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', class_='table-responsive table-striped')
        if not table or not (tbody := table.find('tbody')):
            self.console.log(f"[yellow]Court list table data not found on page {current_page_num}.")
            return []
        page_data = []
        for row in tbody.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 4: continue
            try:
                nama_cell, tinggi_cell, prov_cell, jumlah_cell = cells[:4]
                nama_link = nama_cell.find('a')
                tinggi_link = tinggi_cell.find('a')
                jumlah_raw = jumlah_cell.text.strip()
                numbers_match = re.search(r'(\d[\d.,]*)?(?:\s*/\s*(\d[\d.,]*))?', jumlah_raw)
                putusan_str = numbers_match.group(1).replace('.', '').replace(',', '') if numbers_match and numbers_match.group(1) else '0'
                publikasi_str = numbers_match.group(2).replace('.', '').replace(',', '') if numbers_match and numbers_match.group(2) else '0'
                putusan = int(putusan_str) if putusan_str.isdigit() else None
                publikasi = int(publikasi_str) if publikasi_str.isdigit() else None
                page_data.append({
                    "nama_pengadilan": nama_link.text.strip() if nama_link else nama_cell.text.strip(),
                    "link_pengadilan": nama_link.get('href') if nama_link else None,
                    "pengadilan_tinggi": tinggi_link.text.strip() if tinggi_link else tinggi_cell.text.strip(),
                    "link_pengadilan_tinggi": tinggi_link.get('href') if tinggi_link else None,
                    "provinsi": prov_cell.text.strip(),
                    "jumlah_raw": jumlah_raw,
                    "jumlah_putusan": putusan,
                    "jumlah_publikasi": publikasi,
                })
            except Exception as e:
                self.console.log(f"[red]Error parsing court list row on page {current_page_num}: {e}")
        return page_data

    def get_court_list(self, url=None, start_page=1, max_pages=None, save_output=False, output_file=None):
        # Fetches and parses court list pages
        base_url_provided = url or self.base_url
        data = []
        html_page1 = self._fetch_page(1, base_url_provided)
        if not html_page1:
            self.console.log(f"[red]Failed to fetch initial page for court list: {base_url_provided}")
            return []
        last_page = self._get_last_page(html_page1) or 1
        actual_start_page = max(1, start_page)
        actual_end_page = min(actual_start_page + max_pages - 1, last_page) if max_pages is not None else last_page
        actual_end_page = min(actual_end_page, last_page) # Ensure end_page doesn't exceed last_page

        self.console.log(f"[cyan]Fetching court list from {base_url_provided} - Pages: {actual_start_page} to {actual_end_page} (of {last_page})")

        with Progress(TextColumn("[cyan]{task.description}"), BarColumn(), MofNCompleteColumn(), console=self.console) as progress:
            task = progress.add_task("Scraping Court List", total=(actual_end_page - actual_start_page + 1))
            current_url_base = re.sub(r'/page/\d+\.html$', '', base_url_provided.split('?')[0])

            for page in range(actual_start_page, actual_end_page + 1):
                page_url_to_fetch = f"{current_url_base}/page/{page}.html" if page > 1 else base_url_provided
                page_html = html_page1 if page == 1 and actual_start_page == 1 else self._fetch_page(page, page_url_to_fetch)

                if not page_html:
                    self.console.log(f"[red]Failed to fetch page {page} for court list ({page_url_to_fetch}), stopping.")
                    break
                data.extend(self._parse_data(page_html, page))
                progress.update(task, advance=1)
                if page == 1 and actual_start_page == 1: html_page1 = None # Avoid keeping page 1 html in memory

        if save_output and data:
            output_path = output_file or self.output_file
            try:
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                self.console.log(f"[green]Saved {len(data)} court records to {output_path}")
            except IOError as e:
                 self.console.log(f"[red]Error saving court list data to {output_path}: {e}")
        return data


    def scrape_list_court(self):
        # Manages state and calls get_court_list for full scraping
        self.current_page, self.all_scraped_data = self._load_state()
        start_page_from_state = self.current_page

        initial_html_for_last_page = self._fetch_page(1)
        if not initial_html_for_last_page:
             self.console.log("[bold red]Fatal: Could not fetch page 1 to determine total pages. Exiting.")
             return
        self.last_page = self._get_last_page(initial_html_for_last_page)

        if not self.last_page:
            self.console.log("[bold red]Fatal: Could not determine total pages from page 1. Exiting.")
            return
        self.console.log(f"Total pages determined: {self.last_page}")

        if start_page_from_state > self.last_page:
            self.console.log(f"[green]Scraping already complete (saved state page {start_page_from_state} > last page {self.last_page}). Finalizing.")
        else:
             custom_progress = Progress(
                TextColumn("[progress.description]{task.description}"), BarColumn(), TaskProgressColumn(), MofNCompleteColumn(),
                TextColumn("[cyan]{task.fields[speed]:.2f} rec/s"), "•", TimeRemainingColumn(), "•", TimeElapsedColumn(),
                console=self.console, transient=False )

             with custom_progress as progress:
                task_id = progress.add_task( "[cyan]Processing Court List Pages...", total=self.last_page, completed=start_page_from_state - 1, speed=0.0 )
                total_records_count = len(self.all_scraped_data)
                run_start_time = time.monotonic()

                for page_num in range(start_page_from_state, self.last_page + 1):
                    page_start_time = time.monotonic()
                    html = self._fetch_page(page_num)
                    if not html:
                        self.console.log(f"[red]Failed to fetch page {page_num}. Stopping scrape.")
                        self._save_state(page_num, self.all_scraped_data)
                        self.current_page = page_num
                        break
                    page_data = self._parse_data(html, page_num)
                    # Basic duplicate check based on link_pengadilan if resuming (can be improved)
                    new_data_to_add = []
                    existing_links = {item.get("link_pengadilan") for item in self.all_scraped_data if item.get("link_pengadilan")}
                    for item in page_data:
                        if item.get("link_pengadilan") not in existing_links:
                            new_data_to_add.append(item)
                            existing_links.add(item.get("link_pengadilan")) # Add to set immediately

                    self.all_scraped_data.extend(new_data_to_add)
                    next_page_to_process = page_num + 1
                    self._save_state(next_page_to_process, self.all_scraped_data)
                    duration = time.monotonic() - page_start_time
                    new_records_on_page = len(new_data_to_add) # Count only newly added records
                    total_records_count += new_records_on_page
                    self.console.log(f"Page {page_num} processed in {duration:.2f}s ({new_records_on_page} new records)")
                    run_elapsed = time.monotonic() - run_start_time + 1e-9
                    progress.update(task_id, advance=1, speed=total_records_count/run_elapsed)
                    self.current_page = next_page_to_process

        is_complete = self.current_page > self.last_page
        if is_complete:
            self.console.log(f"[bold green]Scraping finished successfully. Total records: {len(self.all_scraped_data)}")
            try:
                if os.path.exists(self.state_file):
                     os.remove(self.state_file)
                     self.console.log(f"[dim]Removed state file: {self.state_file}")
            except OSError as e:
                 self.console.log(f"[red]Warning: Could not remove state file {self.state_file}: {e}")
        else:
             self.console.log(f"[yellow]Scraping interrupted before page {self.current_page}. Total Records collected: {len(self.all_scraped_data)}. State saved for resuming.")

        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(self.all_scraped_data, f, ensure_ascii=False, indent=4)
            self.console.log(f"[bold green]Final data ({len(self.all_scraped_data)} records) saved to {self.output_file}")
        except Exception as e:
            self.console.log(f"[red]Error saving final data: {e}")

    def scrape(self):
        self.scrape_list_court()

    def get_court_yearly_decisions(self, court_code=None, url=None):
        if not (url or court_code): raise ValueError("Either court_code or url must be provided")
        if not (html := self._fetch_page(1, url or f"https://putusan3.mahkamahagung.go.id/direktori/periode/tahunjenis/putus/pengadilan/{court_code}.html")): return []
        return [
            {"year": m[1], "decision_count": int(c) if (c := re.sub(r'[.,]', '', m[2])).isdigit() else 0, "link": m[0]}
            for m in re.findall(r'<tr>\s*<td><a href="([^"]+)">(\d{4})</a></td>\s*<td><a href="[^"]+">([\d.,]+)</a></td>\s*</tr>', html, re.I)
        ]

    def get_court_decision_categories_by_year(self, url):
        if not url: raise ValueError("URL must be provided")
        if not (html := self._fetch_page(1, url)): return []
        if not (m := re.search(r'<div class="card[^"]*">\s*<div class="card-header.*?Direktori.*?</div>.*?<div class="card-body.*?>(.*?)</div>\s*</div>\s*</div>', html, re.DOTALL | re.I)): return []
        return [{"category": n.strip(), "link": l} for l, n in re.findall(r'<a href="([^"]+)"[^>]*style="color:black"[^>]*>\s*([^<]+?)\s*(?:<span|<)', m.group(1), re.I) if n.strip().lower() != "semua direktori"]

    def get_decision_classifications(self, url):
        if not url: raise ValueError("URL must be provided")
        self.console.log(f"[cyan]Fetching decision classifications from: {url}")
        html = self._fetch_page(1, url)
        if not html: self.console.log("[red]Failed to fetch decision classification page"); return []
        soup = BeautifulSoup(html, 'html.parser')
        klasifikasi_card = _find_card_by_header(soup, 'Klasifikasi')
        if not klasifikasi_card: self.console.log("[yellow]Classification card 'Klasifikasi' not found."); return []
        classifications = _extract_items_from_card(klasifikasi_card)
        self.console.log(f"[green]Successfully extracted {len(classifications)} decision classification records from {url}")
        return classifications

    def get_monthly_decision_counts(self, url):
        if not url: raise ValueError("URL must be provided")
        self.console.log(f"[cyan]Fetching monthly decision counts from: {url}")
        html = self._fetch_page(1, url)
        if not html: self.console.log("[red]Failed to fetch monthly count page"); return []
        soup = BeautifulSoup(html, 'html.parser')
        bulan_card = _find_card_by_header(soup, 'Bulan')
        if not bulan_card: self.console.log("[yellow]Monthly count card 'Bulan' not found."); return []
        monthly_counts = _extract_items_from_card(bulan_card, item_selector='div.form-check')
        self.console.log(f"[green]Successfully extracted {len(monthly_counts)} monthly count records from {url}")
        return monthly_counts

    def get_decision_list(self, url):
        # Gets list of decisions from a directory page
        if not url: raise ValueError("URL must be provided for decision list")
        self.console.log(f"[cyan]Fetching decision list from: {url}")
        html = self._fetch_page(1, url)
        if not html: self.console.log("[red]Failed to fetch decision list page"); return []
        soup = BeautifulSoup(html, 'html.parser')
        decision_list = []
        list_container = soup.select_one('#tabs-1 #popular-post-list-sidebar')
        if not list_container:
            self.console.log("[yellow]Decision list container ('#tabs-1 #popular-post-list-sidebar') not found.")
            no_data = soup.select_one('#tabs-1 #popular-post-list-sidebar .spost .entry-c:contains("Data Tidak Ditemukan")')
            if no_data: self.console.log("[yellow]Page indicates no decision data found.")
            return []
        entries = list_container.find_all('div', class_='spost clearfix', recursive=False)
        if not entries: self.console.log("[yellow]No decision entries ('div.spost.clearfix') found in container."); return []

        for entry in entries:
            entry_data = {"breadcrumbs": [], "register_date": None, "putus_date": None, "upload_date": None, "title": None, "link": None, "description_parties": None, "view_count": 0, "download_count": 0 }
            try:
                entry_c = entry.find('div', class_='entry-c')
                if not entry_c: continue
                breadcrumb_div = entry_c.find('div', class_='small')
                if breadcrumb_div: entry_data["breadcrumbs"] = [a.text.strip() for a in breadcrumb_div.find_all('a')]
                date_div = breadcrumb_div.find_next_sibling('div', class_='small') if breadcrumb_div else None
                if date_div:
                    date_text = date_div.text
                    reg_match = re.search(r'Register\s*:\s*(\d{2}-\d{2}-\d{4})', date_text)
                    putus_match = re.search(r'Putus\s*:\s*(\d{2}-\d{2}-\d{4})', date_text)
                    upload_match = re.search(r'Upload\s*:\s*(\d{2}-\d{2}-\d{4})', date_text)
                    entry_data["register_date"] = reg_match.group(1) if reg_match else None
                    entry_data["putus_date"] = putus_match.group(1) if putus_match else None
                    entry_data["upload_date"] = upload_match.group(1) if upload_match else None
                title_link_tag = entry_c.select_one('strong > a[href]')
                if title_link_tag:
                    entry_data["title"] = title_link_tag.text.strip()
                    entry_data["link"] = title_link_tag.get('href')
                else: self.console.log(f"[yellow]Could not find title link for an entry in {url}.")

                last_div_in_entry = entry_c.find_all('div', recursive=False)[-1] if entry_c.find_all('div', recursive=False) else None
                desc_parts = []
                start_node = title_link_tag.parent if title_link_tag else (date_div if date_div else breadcrumb_div)
                current_node = start_node.find_next_sibling() if start_node else None
                while current_node and current_node != last_div_in_entry:
                    if isinstance(current_node, NavigableString):
                        stripped = current_node.strip()
                        if stripped: desc_parts.append(stripped)
                    elif isinstance(current_node, Tag):
                        if current_node.name == 'br':
                            if desc_parts and desc_parts[-1] != '\n': desc_parts.append('\n')
                        elif current_node.find('i', class_='icon-eye') is None and current_node.find('i', class_='icon-download') is None:
                             stripped = current_node.get_text(strip=True) # Use get_text for potential nested tags
                             if stripped: desc_parts.append(stripped)
                    current_node = current_node.next_sibling
                entry_data["description_parties"] = "\n".join(desc_parts).strip() # Join with newline, then strip ends


                if last_div_in_entry:
                    view_icon = last_div_in_entry.find('i', class_='icon-eye')
                    if view_icon:
                        view_strong = view_icon.find_next_sibling('strong')
                        if view_strong and view_strong.text.strip().isdigit(): entry_data["view_count"] = int(view_strong.text.strip())
                    download_icon = last_div_in_entry.find('i', class_='icon-download')
                    if download_icon:
                        download_strong = download_icon.find_next_sibling('strong')
                        if download_strong and download_strong.text.strip().isdigit(): entry_data["download_count"] = int(download_strong.text.strip())

                if entry_data["title"] is not None: decision_list.append(entry_data)
                else: self.console.log(f"[yellow]Skipping entry due to missing title in {url}")
            except Exception as e:
                self.console.log(f"[red]Error parsing decision list entry in {url}: {e}")
        self.console.log(f"[green]Successfully extracted {len(decision_list)} decision list entries from {url}")
        return decision_list

    def get_decision_detail(self, url):
        if not url: raise ValueError("URL must be provided for decision detail")
        self.console.log(f"[cyan]Fetching decision detail from: {url}")
        html = self._fetch_page(1, url) # page number irrelevant
        if not html: self.console.log("[red]Failed to fetch decision detail page"); return None
        soup = BeautifulSoup(html, 'html.parser')
        details = {}

        # --- Metadata Extraction ---
        metadata_container = soup.select_one('#tabs-1 #popular-post-list-sidebar')
        if not metadata_container:
            self.console.log("[yellow]Metadata container '#tabs-1 #popular-post-list-sidebar' not found.")
            return None
        table = metadata_container.find('table', class_='table')
        if not table:
             self.console.log("[yellow]Metadata table not found within container.")
             # Attempt fallback to get title if table missing
             title_h2 = metadata_container.find('h2')
             if title_h2:
                 details['title_full'] = title_h2.get_text(separator='\n', strip=True)
                 # Attempt to extract parties from span if possible
                 parties_span = title_h2.find('span', id='title_pihak')
                 details['parties_raw'] = parties_span.get_text(separator='\n', strip=True) if parties_span else None
             else:
                 return None # Cannot proceed without title/table
        else:
            # Extract Title and Parties from H2 above table
            title_h2 = table.find_previous_sibling('h2')
            if title_h2:
                details['title_full'] = title_h2.get_text(separator='\n', strip=True)
                parties_span = title_h2.find('span', id='title_pihak')
                details['parties_raw'] = parties_span.get_text(separator='\n', strip=True) if parties_span else None
            else:
                self.console.log("[yellow]Title H2 not found near metadata table.")
                details['title_full'] = None
                details['parties_raw'] = None


            # Extract details from table rows
            rows = table.select('tbody > tr')
            label_map = {
                "nomor": "nomor",
                "tingkat proses": "tingkat_proses",
                "klasifikasi": "klasifikasi",
                "kata kunci": "kata_kunci",
                "tahun": "tahun",
                "tanggal register": "tanggal_register",
                "lembaga peradilan": "lembaga_peradilan", # Special handling for link
                "jenis lembaga peradilan": "jenis_lembaga_peradilan",
                "hakim ketua": "hakim_ketua",
                "hakim anggota": "hakim_anggota",
                "panitera": "panitera",
                "amar": "amar",
                "amar lainnya": "amar_lainnya",
                "catatan amar": "catatan_amar", # Special handling for HTML/text
                "tanggal musyawarah": "tanggal_musyawarah",
                "tanggal dibacakan": "tanggal_dibacakan",
                "kaidah": "kaidah",
                "abstrak": "abstrak" # Special handling for potential HTML
            }

            for row in rows:
                cells = row.find_all('td', recursive=False)
                if len(cells) == 2:
                    label_td, value_td = cells
                    label_text = label_td.text.strip().lower()
                    dict_key = label_map.get(label_text)

                    if dict_key:
                        try:
                            if dict_key == "klasifikasi":
                                details[dict_key] = [a.text.strip() for a in value_td.find_all('a')]
                            elif dict_key == "lembaga_peradilan":
                                link_tag = value_td.find('a')
                                details[dict_key] = link_tag.text.strip() if link_tag else value_td.text.strip()
                                details["lembaga_peradilan_link"] = link_tag['href'] if link_tag else None
                            elif dict_key == "catatan_amar":
                                # Get text content, trying to preserve paragraphs/lists
                                details[dict_key] = value_td.get_text(separator='\n', strip=True)
                            elif dict_key == "abstrak":
                                # Could contain HTML, get text for now
                                details[dict_key] = value_td.get_text(strip=True) # Simple text for now
                            else:
                                value = value_td.text.strip()
                                details[dict_key] = value if value != '—' else None
                        except Exception as e:
                             self.console.log(f"[red]Error parsing metadata row '{label_text}': {e}")
                             details[dict_key] = None # Set to None on error


        # --- Download Links Extraction ---
        details['download_link_zip'] = None
        details['download_link_pdf'] = None
        lampiran_card = _find_card_by_header(soup, 'Lampiran')
        if lampiran_card:
            items = _extract_items_from_card(lampiran_card, item_selector='li', link_selector='a')
            for item in items:
                if item.get('type') == 'zip':
                    details['download_link_zip'] = item.get('link')
                elif item.get('type') == 'pdf':
                    details['download_link_pdf'] = item.get('link')


        self.console.log(f"[green]Successfully extracted decision details from {url}")
        return details