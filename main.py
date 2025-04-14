# main.py
import json
import os
import re
import time
from urllib.parse import urlparse

import requests
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    Progress, BarColumn, TextColumn, TimeRemainingColumn,
    TimeElapsedColumn, MofNCompleteColumn
)

from MahkamahAgungScraper import MahkamahAgungScraper

# --- Configuration ---
STATE_FILE = "scrape_state.json"
COURT_LIST_CACHE_FILE = "court_list_cache.json"
OUTPUT_DATA_FILE = "mahkamah_agung_decisions.jsonl"
OUTPUT_PDF_DIR = "output_data/pdfs"
TARGET_COURT_LIST_URL = "https://putusan3.mahkamahagung.go.id/pengadilan/index/ditjen/umum.html"
MAX_COURTS_TO_PROCESS = None
REQUEST_DELAY = 1

# --- Global State Variable ---
current_state = {} # Stores LAST COMPLETED index

# --- Helper Functions (ensure_dir, load_state, save_state, load_court_list_cache, save_court_list_cache, append_data, _download_pdf_main) ---
# (These remain the same as the previous version - saving state frequently during list fetch)
console = Console()
def ensure_dir(directory_path):
    if not os.path.exists(directory_path): os.makedirs(directory_path); console.log(f"[cyan]Created dir:[/cyan] {directory_path}")

def load_state(filename=STATE_FILE):
    global current_state
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f: current_state = json.load(f)
            console.log(f"[yellow]Resuming state (last completed):[/yellow]", current_state)
            return current_state
        except (json.JSONDecodeError, IOError) as e:
            console.log(f"[red]Err loading state {filename}: {e}. Starting fresh.[/red]")
            try: os.rename(filename, f"{filename}.corrupted_{int(time.time())}")
            except OSError: pass
            current_state = {}; return current_state
    current_state = {}; return current_state

def save_state(filename=STATE_FILE):
    global current_state; state_to_save = current_state.copy()
    try:
        if os.path.exists(filename): # Backup logic
            backup_filename = f"{filename}.bak"
            try: os.replace(filename, backup_filename)
            except OSError:
                 if os.path.exists(backup_filename): os.remove(backup_filename)
                 os.rename(filename, backup_filename)
        with open(filename, 'w', encoding='utf-8') as f: json.dump(state_to_save, f, indent=4, ensure_ascii=False)
        # console.log(f"[grey70]State saved: {state_to_save}[/grey70]")
    except Exception as e: console.log(f"[red]Err saving state: {e}[/red]")

def load_court_list_cache(cache_file=COURT_LIST_CACHE_FILE):
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f: courts = json.load(f)
            console.log(f"[cyan]Loaded {len(courts)} courts from cache: {cache_file}[/cyan]"); return courts
        except (json.JSONDecodeError, IOError) as e:
            console.log(f"[red]Err loading court cache {cache_file}: {e}. Re-fetching.[/red]")
            try: os.remove(cache_file)
            except OSError: pass
    return []

def save_court_list_cache(court_list, cache_file=COURT_LIST_CACHE_FILE):
    try:
        with open(cache_file, 'w', encoding='utf-8') as f: json.dump(court_list, f, ensure_ascii=False)
        # console.log(f"[grey70]Court cache saved ({len(court_list)} items)[/grey70]")
    except IOError as e: console.log(f"[red]Err saving court cache {cache_file}: {e}[/red]")

def append_data(data_record, filename=OUTPUT_DATA_FILE):
    try:
        with open(filename, 'a', encoding='utf-8') as f: json.dump(data_record, f, ensure_ascii=False); f.write('\n')
    except IOError as e: console.log(f"[red]Err appending data {filename}: {e}[/red]")

def _download_pdf_main(scraper_instance, url, output_dir):
    if not url: return None; filepath = None
    try:
        parsed_path = url.split('/')[-1]; filename = f"{parsed_path}.pdf" if not parsed_path.lower().endswith('.pdf') else parsed_path
        filename = re.sub(r'[\\/*?:"<>|]', "_", filename); max_len=150
        if len(filename) > max_len: name, ext = os.path.splitext(filename); filename = name[:max_len - len(ext)] + ext
        filepath = os.path.join(output_dir, filename)
        if os.path.exists(filepath) and os.path.getsize(filepath) > 1000: return filepath
        response = scraper_instance.session.get(url, stream=True, timeout=scraper_instance.timeout + 30); response.raise_for_status()
        with open(filepath, 'wb') as pdf_file:
            for chunk in response.iter_content(chunk_size=8192 * 4): pdf_file.write(chunk)
        console.log(f"[green]PDF downloaded:[/green] {os.path.basename(filepath)}"); return filepath
    except requests.exceptions.RequestException as e:
        if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 404: console.print(f"[yellow]PDF 404: {url}[/yellow]")
        else: console.print(f"[red]Failed DL PDF from {url}: {e}[/red]")
        if filepath and os.path.exists(filepath):
             try: os.remove(filepath)
             except OSError: pass
        return None
    except Exception as e: console.print(f"[red]Unexpected PDF DL err from {url}: {e}[/red]"); return None
# --- End Helpers ---


# --- Main Scraping Logic ---
def run_scraper():
    global current_state
    ensure_dir(OUTPUT_PDF_DIR)
    current_state = load_state()
    scraper = MahkamahAgungScraper(timeout=60, retry_delay=10)

    progress = Progress(
        TextColumn("[progress.description]{task.description}", justify="right"),
        BarColumn(bar_width=None), TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
        MofNCompleteColumn(), TimeElapsedColumn(), TimeRemainingColumn(),
        console=console, expand=True
    )

    try:
        with progress:
            console.print(Panel(f"Starting scrape. State (last completed): {current_state}\nOutput: {OUTPUT_DATA_FILE}, PDFs: {OUTPUT_PDF_DIR}", title="Scraper Initialized", border_style="green"))
            time.sleep(REQUEST_DELAY)

            # 1. Get Court List (with state saving during fetch)
            courts_task_id = progress.add_task("[green]Courts", total=1, start=False)
            last_page_courts = current_state.get('court_list_total_pages', None)
            try: # Fetch court list pages with state
                if last_page_courts is None:
                    console.log(f"Fetching page 1 court list for total pages...")
                    first_page_html = scraper._fetch_page(1, url=TARGET_COURT_LIST_URL)
                    if not first_page_html: raise Exception("Failed fetch page 1 for total pages")
                    last_page_courts = scraper.get_last_page(first_page_html) or 1
                    current_state['court_list_total_pages'] = last_page_courts; save_state()
                    console.log(f"Found {last_page_courts} pages of courts.")
                else: console.log(f"Resuming court list fetch (Total pages: {last_page_courts})")
                all_courts = load_court_list_cache(); last_page_fetched = current_state.get('court_list_last_page_fetched', 0)
                start_fetch_page = last_page_fetched + 1
                if start_fetch_page <= last_page_courts:
                    list_page_task = progress.add_task(f"[magenta]Fetching court pages ({start_fetch_page}/{last_page_courts})", total=last_page_courts, completed=start_fetch_page - 1)
                    for page_num in range(start_fetch_page, last_page_courts + 1):
                        progress.update(list_page_task, description=f"[magenta]Fetching court list page ({page_num}/{last_page_courts})")
                        page_url = f"{TARGET_COURT_LIST_URL}?page={page_num}" if page_num > 1 else TARGET_COURT_LIST_URL
                        try:
                            courts_on_page = scraper.get_list_courts(url=page_url)
                            if courts_on_page: all_courts.extend(courts_on_page)
                            current_state['court_list_last_page_fetched'] = page_num
                            save_court_list_cache(all_courts); save_state() # Save after each page fetch
                            progress.advance(list_page_task)
                            if page_num < last_page_courts: time.sleep(0.3)
                        except Exception as fetch_err: console.print(f"[red]Error fetching court list page {page_num}: {fetch_err}. Stopping list fetch.[/red]"); raise fetch_err
                    progress.remove_task(list_page_task)
                else: console.log("[green]Court list already fully fetched.[/green]")
                if not all_courts: raise Exception("Failed to fetch or load any courts")
                last_completed_court = current_state.get('court_idx', -1); progress.update(courts_task_id, total=len(all_courts), completed=last_completed_court + 1, start=True)
            except Exception as e: console.print(f"[red]Fatal Error fetching court list: {e}"); return

            # ==============================================================
            # Main Processing Loops (Court -> Year -> Cat -> Class -> MONTH -> Page -> Decision)
            # ==============================================================
            start_court_idx = current_state.get('court_idx', -1) + 1
            if start_court_idx >= len(all_courts): console.log("[green]All courts previously completed."); start_court_idx = len(all_courts)
            console.log(f"Starting main court processing from index: {start_court_idx}")

            for court_idx in range(start_court_idx, len(all_courts)):
                court = all_courts[court_idx]; loaded_court_idx = current_state.get('court_idx', -1)
                if loaded_court_idx < court_idx - 1: # Clear inner states if not resuming this court
                     current_state.pop('year_idx', None); current_state.pop('category_idx', None); current_state.pop('classification_idx', None); current_state.pop('month_idx', None); current_state.pop('decision_page', None)

                current_court_name = court.get('nama_pengadilan', f'?C {court_idx+1}'); current_court_link = court.get('link_pengadilan')
                court_task_desc = f"[green]Court {court_idx+1}/{len(all_courts)}:[/green] {current_court_name}"; progress.update(courts_task_id, description=court_task_desc)
                court_skipped = False; court_code = None
                if not current_court_link: court_skipped = True; console.log(f"[yellow]Skip Court (no link)")
                else: # Extract code
                    try: parsed_url=urlparse(current_court_link); p=[p for p in parsed_url.path.split('/') if p]; pc=p[-1].replace('.html','') if p else ''; court_code=pc if any(x in pc for x in ['pn-','pt-','pa-','ma-','tun-']) else None
                    except Exception: pass
                    if not court_code: court_skipped = True; console.log(f"[yellow]Skip Court (no code)")
                if court_skipped: current_state['court_idx'] = court_idx; save_state(); progress.advance(courts_task_id); continue

                # --- Process Years ---
                years_task_id = progress.add_task(f"  Years ({current_court_name})", total=1, start=False, visible=True)
                yearly_decisions = []; start_year_idx = current_state.get('year_idx', -1) + 1
                try:
                    time.sleep(REQUEST_DELAY*0.8); yearly_decisions = scraper.get_court_yearly_decisions(court_code=court_code)
                    if not yearly_decisions: court_skipped = True; progress.update(years_task_id, visible=False); console.log(f"[grey50]No Years found for {current_court_name}[/grey50]")
                    else:
                        if start_year_idx >= len(yearly_decisions): start_year_idx = len(yearly_decisions)
                        progress.update(years_task_id, total=len(yearly_decisions), completed=start_year_idx, start=True)
                except Exception as e: court_skipped = True; console.print(f"[red]Err Years: {e}"); progress.update(years_task_id, visible=False)

                if not court_skipped:
                    for year_idx in range(start_year_idx, len(yearly_decisions)):
                        year_data = yearly_decisions[year_idx]; loaded_year_idx = current_state.get('year_idx', -1)
                        if loaded_year_idx < year_idx - 1: current_state.pop('category_idx', None); current_state.pop('classification_idx', None); current_state.pop('month_idx', None); current_state.pop('decision_page', None)
                        current_year = year_data.get('year', f'?Y {year_idx+1}'); year_link = year_data.get('link')
                        year_task_desc = f"  Year {year_idx+1}/{len(yearly_decisions)}: {current_year}"; progress.update(years_task_id, description=year_task_desc); year_skipped = False
                        if not year_link: year_skipped = True; console.log(f"[yellow]Skip Year (no link)")
                        else:
                            # --- Process Categories ---
                            cats_task_id = progress.add_task(f"    Cats ({current_year})", total=1, start=False, visible=True)
                            categories = []; start_cat_idx = current_state.get('category_idx', -1) + 1
                            try:
                                time.sleep(REQUEST_DELAY*0.8); categories = scraper.get_court_decision_categories_by_year(url=year_link)
                                if not categories: year_skipped = True; progress.update(cats_task_id, visible=False); console.log(f"[grey50]No Cats found for {current_year}[/grey50]")
                                else:
                                    if start_cat_idx >= len(categories): start_cat_idx = len(categories)
                                    progress.update(cats_task_id, total=len(categories), completed=start_cat_idx, start=True)
                            except Exception as e: year_skipped = True; console.print(f"[red]Err Cats: {e}"); progress.update(cats_task_id, visible=False)

                            if not year_skipped:
                                for cat_idx in range(start_cat_idx, len(categories)):
                                    category_data = categories[cat_idx]; loaded_cat_idx = current_state.get('category_idx', -1)
                                    if loaded_cat_idx < cat_idx - 1: current_state.pop('classification_idx', None); current_state.pop('month_idx', None); current_state.pop('decision_page', None)
                                    current_category = category_data.get('category', f'?Cat{cat_idx+1}'); category_link = category_data.get('link')
                                    cat_task_desc = f"    Cat {cat_idx+1}/{len(categories)}: {current_category}"; progress.update(cats_task_id, description=cat_task_desc); cat_skipped = False
                                    if not category_link: cat_skipped = True; console.log(f"[yellow]Skip Cat (no link)")
                                    else:
                                        # --- Process Classifications ---
                                        class_task_id = progress.add_task(f"      Class ({current_category})", total=1, start=False, visible=True)
                                        classifications = []; start_class_idx = current_state.get('classification_idx', -1) + 1
                                        try:
                                            time.sleep(REQUEST_DELAY*0.8); classifications = scraper.get_decision_classifications(url=category_link)
                                            if not classifications: cat_skipped = True; progress.update(class_task_id, visible=False); console.log(f"[grey50]No Class found for {current_category}[/grey50]")
                                            else:
                                                if start_class_idx >= len(classifications): start_class_idx = len(classifications)
                                                progress.update(class_task_id, total=len(classifications), completed=start_class_idx, start=True)
                                        except Exception as e: cat_skipped = True; console.print(f"[red]Err Class: {e}"); progress.update(class_task_id, visible=False)

                                        if not cat_skipped:
                                            for class_idx in range(start_class_idx, len(classifications)):
                                                classification_data = classifications[class_idx]; loaded_class_idx = current_state.get('classification_idx', -1)
                                                if loaded_class_idx < class_idx - 1: current_state.pop('month_idx', None); current_state.pop('decision_page', None)
                                                current_classification = classification_data.get('classification', f'?Cls{class_idx+1}'); classification_link = classification_data.get('link')
                                                class_task_desc = f"      Class {class_idx+1}/{len(classifications)}: {current_classification}"; progress.update(class_task_id, description=class_task_desc); class_skipped = False
                                                if not classification_link: class_skipped = True; console.log(f"[yellow]Skip Class (no link)")
                                                else:
                                                    # --- Process MONTHS (New Step!) ---
                                                    months_task_id = progress.add_task(f"        Months ({current_classification[:15]}..)", total=1, start=False, visible=True)
                                                    monthly_counts = []; start_month_idx = current_state.get('month_idx', -1) + 1
                                                    try:
                                                        time.sleep(REQUEST_DELAY * 0.7) # Slightly less delay
                                                        monthly_counts = scraper.get_monthly_decision_counts(url=classification_link)
                                                        if not monthly_counts: class_skipped = True; progress.update(months_task_id, visible=False); console.log(f"[grey50]No Months found for {current_classification}[/grey50]")
                                                        else:
                                                            if start_month_idx >= len(monthly_counts): start_month_idx = len(monthly_counts)
                                                            progress.update(months_task_id, total=len(monthly_counts), completed=start_month_idx, start=True)
                                                    except Exception as e: class_skipped = True; console.print(f"[red]Err Months: {e}"); progress.update(months_task_id, visible=False)

                                                    if not class_skipped:
                                                        for month_idx in range(start_month_idx, len(monthly_counts)):
                                                            month_data = monthly_counts[month_idx]; loaded_month_idx = current_state.get('month_idx', -1)
                                                            if loaded_month_idx < month_idx - 1: current_state.pop('decision_page', None) # Clear page state for new month
                                                            current_month_name = month_data.get('month', f'?Mon{month_idx+1}')
                                                            month_task_desc = f"        Month {month_idx+1}/{len(monthly_counts)}: {current_month_name}"; progress.update(months_task_id, description=month_task_desc); month_skipped = False

                                                            # --- Process Pages (using classification_link) ---
                                                            pages_task_id = progress.add_task(f"          Pages ({current_month_name})", total=1, start=False, visible=True)
                                                            last_page = 1;
                                                            start_page = current_state.get('decision_page', 0) + 1
                                                            try:
                                                                # We still need pagination info from the classification link
                                                                time.sleep(REQUEST_DELAY * 0.7)
                                                                first_page_html_content = scraper._fetch_page(1, url=classification_link)
                                                                if not first_page_html_content: raise Exception("Failed fetch page 1 for pagination")
                                                                last_page = scraper.get_last_page(first_page_html_content) or 1
                                                                if start_page > last_page: start_page = last_page + 1
                                                                progress.update(pages_task_id, total=last_page, completed=start_page - 1, start=True)
                                                            except Exception as e: month_skipped = True; console.print(f"[red]Err Pages Info: {e}"); progress.update(pages_task_id, visible=False)

                                                            if not month_skipped:
                                                                for page_num in range(start_page, last_page + 1):
                                                                    page_task_desc = f"          Page {page_num}/{last_page} ({current_month_name})"; progress.update(pages_task_id, description=page_task_desc); page_skipped = False
                                                                    page_url = f"{classification_link}?page={page_num}" if page_num > 1 else classification_link

                                                                    # --- Process Decisions ---
                                                                    decisions_task_id = progress.add_task(f"            Decisions (Pg {page_num})", total=1, start=False, visible=True)
                                                                    decisions_on_page = []
                                                                    try:
                                                                        time.sleep(REQUEST_DELAY*0.6); decisions_on_page = scraper.get_decision_list(url=page_url)
                                                                        if not decisions_on_page: progress.update(decisions_task_id, visible=False) # No decisions is fine
                                                                        else: progress.update(decisions_task_id, total=len(decisions_on_page), completed=0, start=True)
                                                                    except Exception as e: page_skipped = True; console.print(f"[red]Err Decisions: {e}"); progress.update(decisions_task_id, visible=False)

                                                                    if not page_skipped and decisions_on_page:
                                                                        for decision_idx, decision_summary in enumerate(decisions_on_page):
                                                                            # Get detail, append, download PDF (same as before)
                                                                            decision_link = decision_summary.get('link'); decision_title = decision_summary.get('title', '?Dec')
                                                                            display_title = (decision_title[:35] + '...') if len(decision_title) > 38 else decision_title
                                                                            dec_task_desc = f"            Decision {decision_idx+1}/{len(decisions_on_page)}: {display_title}"; progress.update(decisions_task_id, description=dec_task_desc)
                                                                            if decision_link:
                                                                                try:
                                                                                    time.sleep(REQUEST_DELAY*0.5); decision_detail = scraper.get_decision_detail(url=decision_link)
                                                                                    if decision_detail:
                                                                                        decision_detail['_source_court_name'] = current_court_name; decision_detail['_source_court_code'] = court_code; decision_detail['_source_year'] = current_year; decision_detail['_source_category'] = current_category; decision_detail['_source_classification'] = current_classification; decision_detail['_source_month'] = current_month_name; decision_detail['_source_decision_list_url'] = page_url; decision_detail['_source_decision_detail_url'] = decision_link; decision_detail['_scrape_timestamp'] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
                                                                                        append_data(decision_detail, OUTPUT_DATA_FILE)
                                                                                        pdf_url = decision_detail.get('download_link_pdf')
                                                                                        if pdf_url: time.sleep(REQUEST_DELAY*0.3); _download_pdf_main(scraper, pdf_url, OUTPUT_PDF_DIR)
                                                                                except Exception as e: console.print(f"[red]Err detail/DL ({decision_link}): {e}")
                                                                            progress.advance(decisions_task_id) # Advance per decision attempt
                                                                        progress.update(decisions_task_id, visible=False) # Hide when page decisions done
                                                                    # --- End Decision Processing ---
                                                                    current_state['decision_page'] = page_num; save_state(); progress.advance(pages_task_id)
                                                                    if page_skipped: break # Exit page loop for this month
                                                                progress.update(pages_task_id, visible=False) # Hide page bar when month done
                                                            # --- End Page Processing ---
                                                            current_state['month_idx'] = month_idx; current_state.pop('decision_page', None); save_state(); progress.advance(months_task_id)
                                                            if month_skipped: break # Exit month loop for this class
                                                        progress.update(months_task_id, visible=False) # Hide month bar when class done
                                                    # --- End Month Processing ---
                                                # --- End IF Class Link Exists ---
                                                current_state['classification_idx'] = class_idx; current_state.pop('month_idx', None); current_state.pop('decision_page', None); save_state(); progress.advance(class_task_id)
                                                if class_skipped: break # Exit class loop for this cat
                                            progress.update(class_task_id, visible=False) # Hide class bar when cat done
                                        # --- End Classification Processing ---
                                    # --- End IF Cat Link Exists ---
                                    current_state['category_idx'] = cat_idx; current_state.pop('classification_idx', None); current_state.pop('month_idx', None); current_state.pop('decision_page', None); save_state(); progress.advance(cats_task_id)
                                    if cat_skipped: break # Exit cat loop for this year
                                progress.update(cats_task_id, visible=False) # Hide cat bar when year done
                            # --- End Category Processing ---
                        # --- End IF Year Link Exists ---
                        current_state['year_idx'] = year_idx; current_state.pop('category_idx', None); current_state.pop('classification_idx', None); current_state.pop('month_idx', None); current_state.pop('decision_page', None); save_state(); progress.advance(years_task_id)
                        if year_skipped: break # Exit year loop for this court
                    progress.update(years_task_id, visible=False) # Hide year bar when court done
                # --- End Year Processing ---
                current_state['court_idx'] = court_idx; current_state.pop('year_idx', None); current_state.pop('category_idx', None); current_state.pop('classification_idx', None); current_state.pop('month_idx', None); current_state.pop('decision_page', None); save_state(); progress.advance(courts_task_id)
            # --- End Court Loop ---

            # --- Scraping Finished ---
            progress.update(courts_task_id, description="[bold green]All Courts Processed", completed=len(all_courts))
            console.print(Panel("[bold green]Scraping process completed successfully![/bold green]", title="Finished", border_style="green"))
            try: # Cleanup
                if os.path.exists(COURT_LIST_CACHE_FILE): os.remove(COURT_LIST_CACHE_FILE)
                if os.path.exists(STATE_FILE): os.remove(STATE_FILE)
                if os.path.exists(f"{STATE_FILE}.bak"): os.remove(f"{STATE_FILE}.bak")
                console.log("[green]State and cache files removed on success.[/green]")
            except OSError as e: console.log(f"[yellow]Could not remove state/cache: {e}[/yellow]")

    except KeyboardInterrupt: console.print("\n[yellow]Interrupted. Saving final state...[/yellow]"); save_state(); console.print("[yellow]State saved. Exiting.[/yellow]")
    except Exception: console.print(f"\n[bold red]Unexpected error:[/bold red]"); console.print_exception(show_locals=False); console.print("[yellow]Attempting save state...[/yellow]"); save_state(); console.print("[red]State saved (if possible). Check logs.[/red]")
    finally: console.print("[grey50]Scraper finished or exited.[/grey50]")

if __name__ == "__main__":
    run_scraper()