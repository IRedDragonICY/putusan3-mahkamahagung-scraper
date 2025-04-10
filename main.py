import json
import os
import time
import requests
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

BASE_URL = "https://putusan3.mahkamahagung.go.id/pengadilan.html"
PARAMS = {"ditjen": "umum"}
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
STATE_FILE = "scrape_state.json"
OUTPUT_FILE = "mahkamah_agung_courts.json"
REQUEST_TIMEOUT = 60
RETRY_DELAY = 5

console = Console()

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                state = json.load(f)
                next_page = max(1, state.get('next_page', 1))
                scraped_data = state.get('scraped_data', [])
                console.log(f"[yellow]Resuming from page {next_page}...")
                return next_page, scraped_data
        except (json.JSONDecodeError, IOError, KeyError) as e:
            console.log(f"[red]Error loading state file '{STATE_FILE}': {e}. Starting from scratch.")
            try:
                os.remove(STATE_FILE)
                console.log(f"[yellow]Removed corrupted state file '{STATE_FILE}'.")
            except OSError:
                 pass
            return 1, []
    return 1, []

def save_state(page_to_save, data_to_save):
    state = {
        "next_page": page_to_save,
        "scraped_data": data_to_save
    }
    try:
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
    except IOError as e:
        console.log(f"[red]Warning: Could not save state to '{STATE_FILE}': {e}")

def fetch_page(page_number):
    params = PARAMS.copy()
    params['page'] = page_number
    attempt = 0
    while True:
        attempt += 1
        try:
            response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.exceptions.Timeout:
            console.log(
                f"[yellow]Timeout fetching page {page_number}, attempt {attempt}. Retrying in {RETRY_DELAY}s...")
        except requests.exceptions.RequestException as e:
            console.log(
                f"[red]Error fetching page {page_number}, attempt {attempt}: {e}. Retrying in {RETRY_DELAY}s...")
        time.sleep(RETRY_DELAY)

def get_last_page(html_content):
    if not html_content:
        return None
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        pagination = soup.find('ul', class_='pagination')
        if not pagination:
            console.log("[yellow]Pagination controls not found. Assuming only one page.")
            return 1

        last_link = pagination.find('a', string='Last')
        if last_link and 'data-ci-pagination-page' in last_link.attrs:
            try:
                return int(last_link['data-ci-pagination-page'])
            except (ValueError, TypeError):
                console.log("[yellow]Could not parse 'Last' link page number attribute. Trying other methods.")

        page_links = pagination.find_all('a', {'data-ci-pagination-page': True})
        if page_links:
            last_page_num = 0
            for link in page_links:
                try:
                    page_num = int(link['data-ci-pagination-page'])
                    if page_num > last_page_num:
                        last_page_num = page_num
                except (ValueError, KeyError, TypeError):
                    continue
            if last_page_num > 0:
                return last_page_num
            else:
                 console.log("[yellow]Could not determine last page number from link attributes. Checking active element.")

        active_link = pagination.find('li', class_='active')
        if active_link and active_link.find('a'):
             active_page_text = active_link.find('a').text.strip()
             if active_page_text.isdigit():
                 console.log("[yellow]Determined page number from active element. This might be the last page if 'Last' or numeric links weren't found.")
                 return int(active_page_text)

        console.log("[red]Unable to determine last page number reliably from pagination.")
        return None

    except Exception as e:
        console.log(f"[red]Error parsing pagination structure: {e}")
        return None

def parse_data(html_content, current_page):
    if not html_content:
        return []
    page_data = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', class_='table-responsive table-striped')
        if not table:
            console.log(f"[yellow]Data table not found on page {current_page}.")
            return []
        tbody = table.find('tbody')
        if not tbody:
            console.log(f"[yellow]Table body (tbody) not found on page {current_page}.")
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
                         # Handle cases where only one number might be present without a slash
                         cleaned_raw = jumlah_raw.replace('.', '')
                         if cleaned_raw.isdigit():
                             jumlah_putusan = int(cleaned_raw) # Assume it's 'jumlah putusan' if only one number


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
                    console.log(f"[yellow]Skipping row {i+1} on page {current_page} due to parsing error: {cell_error}. Row content: {row.text[:100]}...")
            else:
                console.log(f"[yellow]Skipping row {i+1} on page {current_page} due to insufficient cells ({len(cells)} found).")
    except Exception as e:
        console.log(f"[red]Error parsing table data on page {current_page}: {e}")
    return page_data


if __name__ == "__main__":
    start_page, all_scraped_data = load_state()
    last_page = None
    initial_html = None
    processed_current_page_data = False

    if start_page > 1:
        console.log(f"Attempting to fetch resume page {start_page} to re-check total pages...")
        initial_html = fetch_page(start_page)
        if initial_html:
            last_page = get_last_page(initial_html)
            console.log(f"Total pages re-checked: {last_page if last_page else 'Could not determine'}")
        else:
            console.log(f"[bold red]Fatal: Could not fetch resume page {start_page}. Exiting.")
            exit(1)
    else:
        console.log("Starting fresh. Fetching page 1 to determine total pages...")
        initial_html = fetch_page(1)
        if initial_html:
            last_page = get_last_page(initial_html)
            if last_page is not None and not all_scraped_data:
                page_1_data = parse_data(initial_html, 1)
                all_scraped_data.extend(page_1_data)
                processed_current_page_data = True
                console.log(f"Page 1 processed initially ({len(page_1_data)} records).")
                save_state(2, all_scraped_data)
                start_page = 2
            elif last_page is None:
                 console.log("[bold red]Fatal: Could not determine total pages from page 1. Exiting.")
                 exit(1)

        else:
            console.log("[bold red]Fatal: Could not fetch page 1. Exiting.")
            exit(1)

    if last_page is None:
        console.log("[bold red]Fatal: Could not determine the total number of pages after initial fetch. Exiting.")
        exit(1)

    console.log(f"Total pages determined: {last_page}")

    actual_start_page_for_loop = start_page
    if processed_current_page_data and start_page == 2:
         console.log("Adjusting loop start as page 1 was processed during init.")
    elif start_page > 1 and initial_html:
         console.log(f"Page {start_page} fetched during init will be processed first.")
    elif start_page == 1 and initial_html and not processed_current_page_data:
         console.log(f"Page {start_page} fetched during init will be processed first.")
         actual_start_page_for_loop = 1
    else:
         actual_start_page_for_loop = start_page


    if actual_start_page_for_loop > last_page:
        console.log("[green]Scraping appears to be already complete based on saved state and total pages.")
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
            console=console,
            transient=False
        )

        with custom_progress as progress:
            task_id = progress.add_task(
                "[cyan]Processing Pages...",
                total=last_page,
                completed=actual_start_page_for_loop - 1,
                speed=0.0
            )

            total_records_processed_in_run = 0
            run_start_time = time.monotonic()

            for current_page in range(actual_start_page_for_loop, last_page + 1):
                page_start_time = time.monotonic()
                html = None

                if current_page == start_page and initial_html:
                     html = initial_html
                     console.log(f"Using pre-fetched content for page {current_page}.")
                else:
                    html = fetch_page(current_page)

                page_data = []
                if html:
                    page_data = parse_data(html, current_page)
                    all_scraped_data.extend(page_data)
                    save_state(current_page + 1, all_scraped_data)
                    page_end_time = time.monotonic()
                    duration = page_end_time - page_start_time
                    total_records_processed_in_run += len(page_data)
                    console.log(f"Page {current_page} processed in {duration:.2f}s ({len(page_data)} records). State saved.")
                else:
                    console.log(f"[red]Fetch/Parse failed for page {current_page}. State not saved for this page. Check logs above.")
                    # Optionally break or continue based on required robustness
                    # For now, continue to next page but progress won't advance fully

                run_elapsed_time = time.monotonic() - run_start_time + 1e-9 # Avoid division by zero
                current_speed = total_records_processed_in_run / run_elapsed_time

                progress.update(task_id, advance=1, speed=current_speed)
                initial_html = None # Ensure fresh fetch for next loop iteration

    final_next_page = (start_page if actual_start_page_for_loop > last_page
                      else (current_page + 1 if 'current_page' in locals() else actual_start_page_for_loop) )

    if final_next_page > last_page:
         console.log(f"[bold green]Scraping finished. Processed up to page {last_page}. Total records collected: {len(all_scraped_data)}")
         if os.path.exists(STATE_FILE):
            try:
                os.remove(STATE_FILE)
                console.log(f"[blue]State file {STATE_FILE} removed as scraping completed.")
            except OSError as e:
                console.log(f"[red]Warning: Could not remove state file {STATE_FILE}: {e}")
    else:
         console.log(f"[yellow]Scraping stopped or was interrupted. Last fully processed page implies next page is {final_next_page}. State file {STATE_FILE} kept for resuming. Total records collected so far: {len(all_scraped_data)}")


    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_scraped_data, f, ensure_ascii=False, indent=4)
        console.log(f"[bold green]Data successfully saved to {OUTPUT_FILE}")
    except IOError as e:
        console.log(f"[red]Error saving final data to {OUTPUT_FILE}: {e}")
    except Exception as e:
         console.log(f"[red]An unexpected error occurred during final data saving: {e}")