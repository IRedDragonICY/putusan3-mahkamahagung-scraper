import json
import os
import time
import requests
import re
from bs4 import BeautifulSoup, Tag, NavigableString
from rich.console import Console



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
    def get_last_page(html_content):
        if not html_content: return None
        soup = BeautifulSoup(html_content, 'lxml')
        pages = [int(a['data-ci-pagination-page'])
                 for a in soup.select('ul.pagination a[data-ci-pagination-page]')
                 if a.get('data-ci-pagination-page', '').isdigit()]
        return max(pages) if pages else 1

    def get_list_courts(self, url=None):
        html_content = self._fetch_page(1, url=url)
        if not html_content: return []
        soup = BeautifulSoup(html_content, 'lxml')
        court_data = []
        for row in soup.select('table.table-responsive.table-striped tbody tr'):
            cells = row.select('td')
            if len(cells) != 4: continue
            nama_tag = cells[0].select_one('a')
            tinggi_tag = cells[1].select_one('a')
            if not nama_tag or not tinggi_tag: continue

            jumlah_putusan, jumlah_publikasi = None, None
            match = re.match(r'([\d,.]+)\s*/\s*([\d,.]+)', cells[3].text.strip())
            if match:
                try:
                    ps = re.sub(r'\D', '', match.group(1))
                    pubs = re.sub(r'\D', '', match.group(2))
                    if ps: jumlah_putusan = int(ps)
                    if pubs: jumlah_publikasi = int(pubs)
                except ValueError: pass

            court_data.append({
                "nama_pengadilan": nama_tag.text.strip(),
                "link_pengadilan": nama_tag.get('href'),
                "pengadilan_tinggi": tinggi_tag.text.strip(),
                "link_pengadilan_tinggi": tinggi_tag.get('href'),
                "provinsi": cells[2].text.strip(),
                "jumlah_putusan": jumlah_putusan,
                "jumlah_publikasi": jumlah_publikasi,
            })
        return court_data

    def get_court_yearly_decisions(self, court_code=None, url=None):
        if not (url or court_code): raise ValueError("Either court_code or url must be provided")
        if not (html := self._fetch_page(1, url or f"https://putusan3.mahkamahagung.go.id/direktori/periode/tahunjenis/putus/pengadilan/{court_code}.html")): return []
        soup = BeautifulSoup(html, 'lxml')
        tbody = soup.select_one('table.table-striped tbody')
        if not tbody: return []
        return [
            {
                "year": links[0].text.strip(),
                "decision_count": int(c) if (c := re.sub(r'[.,]', '', links[1].text.strip())).isdigit() else 0,
                "link": links[0].get('href')
            }
            for row in tbody.select('tr')
            if (links := row.select('td > a[href]')) and len(links) == 2 and links[0].text.strip().isdigit()
        ]

    def get_court_decision_categories_by_year(self, url):
        if not url: raise ValueError("URL must be provided")
        if not (html := self._fetch_page(1, url)): return []
        soup = BeautifulSoup(html, 'lxml')
        card = soup.select_one('div.card:has(> div.card-header :-soup-contains("Direktori"))')
        if not card: return []
        return [
            {"category": name, "link": tag.get('href')}
            for tag in card.select('div.card-body a[href][style*="color:black"]')
            if (name := next(tag.stripped_strings, None)) and name.lower() != "semua direktori"
        ]

    def get_decision_classifications(self, url):
        if not url: raise ValueError("URL must be provided")
        if not (html := self._fetch_page(1, url)): return []
        soup = BeautifulSoup(html, 'lxml')
        card = soup.select_one('div.card:has(> div.card-header :-soup-contains("Klasifikasi"))')
        if not card: return []
        return [
            {"classification": name, "link": tag.get('href')}
            for tag in card.select('div.card-body a[href]')
            if (name := next(tag.stripped_strings, None))
        ]

    def get_monthly_decision_counts(self, url):
        if not url: raise ValueError("URL must be provided")
        if not (html := self._fetch_page(1, url)): return []
        soup = BeautifulSoup(html, 'lxml')
        card = soup.select_one('div.card:has(> div.card-header :-soup-contains("Bulan"))')
        if not card: return []
        return [
            {"month": month_text, "count": int(count_text)}
            for p_tag in card.select('div.card-body div.form-check p.card-text')
            if (span := p_tag.find('span', class_='badge'))
            and (count_text := span.text.strip()).isdigit()
            and (prev_node := span.find_previous(string=True))
            and (month_text := prev_node.strip())
        ]

    def get_decision_list(self, url):
        if not url: raise ValueError("URL must be provided for decision list")
        if not (html := self._fetch_page(1, url)): return []
        soup = BeautifulSoup(html, 'lxml')
        container = soup.select_one('#popular-post-list-sidebar')
        if not container: return []

        return [
            {
                 "breadcrumbs": [a.text.strip() for a in entry_c.select('div.small:first-of-type a')] if entry_c.select_one('div.small:first-of-type') else [],
                 "register_date": (m.group(1) if (m := re.search(r'Register\s*:\s*(\d{2}-\d{2}-\d{4})', date_text)) else None),
                 "putus_date": (m.group(1) if (m := re.search(r'Putus\s*:\s*(\d{2}-\d{2}-\d{4})', date_text)) else None),
                 "upload_date": (m.group(1) if (m := re.search(r'Upload\s*:\s*(\d{2}-\d{2}-\d{4})', date_text)) else None),
                 "title": title,
                 "link": title_tag.get('href'),
                 "description_parties": "\n".join(
                     txt.strip() for txt in (
                         (node.strip() if isinstance(node, NavigableString) else ('\n' if node.name == 'br' else node.get_text(strip=True)))
                         for node in last_div.contents
                         if not (isinstance(node, Tag) and node.find(lambda tag: tag.name == 'i' and ('icon-eye' in tag.get('class', []) or 'icon-download' in tag.get('class', []))))
                     ) if txt
                 ).strip() if last_div else '',
                 "view_count": (int(vt) if last_div and (vs := last_div.select_one('i.icon-eye + strong')) and (vt := vs.text.strip()).isdigit() else 0),
                 "download_count": (int(dt) if last_div and (ds := last_div.select_one('i.icon-download + strong')) and (dt := ds.text.strip()).isdigit() else 0),
             }
            for entry in container.select('div.spost.clearfix')
            if (entry_c := entry.select_one('div.entry-c'))
            and not entry_c.select_one('div.small:contains("Data Tidak Ditemukan")')
            and (title_tag := entry_c.select_one('strong > a[href]'))
            and (title := title_tag.text.strip())
            and (date_text := (d.text if (d := entry_c.select_one('div.small:nth-of-type(2)')) else '')) is not None
            and (last_div := entry_c.select_one('div:last-of-type')) is not None
        ]

    def get_decision_detail(self, url):
        if not url: raise ValueError("URL must be provided for decision detail")
        self.console.log(f"[cyan]Fetching decision detail from: {url}")
        html = self._fetch_page(1, url)
        if not html:
            self.console.log("[red]Failed to fetch decision detail page")
            return None
        soup = BeautifulSoup(html, 'lxml')
        details = {}

        metadata_container = soup.select_one('#tabs-1 #popular-post-list-sidebar')
        if not metadata_container:
            self.console.log("[yellow]Metadata container '#tabs-1 #popular-post-list-sidebar' not found.")
            return None

        title_h2 = metadata_container.find('h2')
        if title_h2:
            details['title_full'] = title_h2.get_text(separator='\n', strip=True)
            parties_span = title_h2.find('span', id='title_pihak')
            details['parties_raw'] = parties_span.get_text(separator='\n', strip=True) if parties_span else None
        else:
             self.console.log("[yellow]Title H2 not found near metadata.")
             details['title_full'] = None
             details['parties_raw'] = None

        table = metadata_container.find('table', class_='table')
        if table:
            rows = table.select('tbody > tr')
            label_map = {
                "nomor": "nomor", "tingkat proses": "tingkat_proses", "klasifikasi": "klasifikasi",
                "kata kunci": "kata_kunci", "tahun": "tahun", "tanggal register": "tanggal_register",
                "lembaga peradilan": "lembaga_peradilan", "jenis lembaga peradilan": "jenis_lembaga_peradilan",
                "hakim ketua": "hakim_ketua", "hakim anggota": "hakim_anggota", "panitera": "panitera",
                "amar": "amar", "amar lainnya": "amar_lainnya", "catatan amar": "catatan_amar",
                "tanggal musyawarah": "tanggal_musyawarah", "tanggal dibacakan": "tanggal_dibacakan",
                "kaidah": "kaidah", "abstrak": "abstrak"
            }
            for row in rows:
                cells = row.find_all('td', recursive=False)
                if len(cells) == 2:
                    label_td, value_td = cells
                    label_text = label_td.text.strip().lower()
                    dict_key = label_map.get(label_text)
                    if dict_key:
                        try:
                            if dict_key == "klasifikasi": details[dict_key] = [a.text.strip() for a in value_td.find_all('a')]
                            elif dict_key == "lembaga_peradilan":
                                link_tag = value_td.find('a')
                                details[dict_key] = link_tag.text.strip() if link_tag else value_td.text.strip()
                                details["lembaga_peradilan_link"] = link_tag['href'] if link_tag else None
                            elif dict_key in ["catatan_amar", "abstrak"]: details[dict_key] = value_td.get_text(separator='\n' if dict_key == "catatan_amar" else '', strip=True)
                            else: value = value_td.text.strip(); details[dict_key] = value if value != '—' else None
                        except Exception as e:
                             self.console.log(f"[red]Error parsing metadata row '{label_text}': {e}")
                             details[dict_key] = None
        elif not details.get('title_full'):
             self.console.log("[yellow]Metadata table not found and no title fallback available.")
             return None
        else:
            self.console.log("[yellow]Metadata table not found within container, using only H2 data if found.")

        details['download_link_zip'] = None
        details['download_link_pdf'] = None
        if lampiran_card := soup.select_one('div.card:has(div.card-header div.togglet:-soup-contains("Lampiran"))'):
            if zip_link_tag := lampiran_card.select_one('ul.portfolio-meta a[href*="/zip/"]'): details['download_link_zip'] = zip_link_tag.get('href')
            if pdf_link_tag := lampiran_card.select_one('ul.portfolio-meta a[href*="/pdf/"]'): details['download_link_pdf'] = pdf_link_tag.get('href')

        self.console.log(f"[green]Successfully extracted decision details from {url}")
        return details