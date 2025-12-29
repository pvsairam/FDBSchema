"""
Fusion DB Schema - Scraper Module
Handles safe, polite scraping of Oracle Fusion documentation.
Uses requests for simple pages, Playwright for JavaScript-heavy pages.
"""
import time
import random
import re
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import database as db

USER_AGENT = "FusionDBSchema/1.0 (Documentation Ingestion Tool for NL2SQL)"
HEADERS = {"User-Agent": USER_AGENT}

class FusionScraper:
    def __init__(self):
        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None
        self._should_stop = False
    
    def start_browser(self):
        db.add_log("INFO", "Using requests library (no browser needed)")
    
    def stop_browser(self):
        pass
    
    def request_stop(self):
        self._should_stop = True
    
    def reset_stop(self):
        self._should_stop = False
    
    def polite_delay(self):
        delay = random.uniform(7, 10)
        db.add_log("INFO", f"Polite delay: waiting {delay:.1f} seconds")
        time.sleep(delay)
    
    def fetch_page(self, url):
        try:
            db.add_log("INFO", f"Fetching: {url}")
            response = requests.get(url, headers=HEADERS, timeout=60)
            response.raise_for_status()
            return response.text
        except Exception as e:
            db.add_log("ERROR", f"Failed to fetch {url}: {str(e)}")
            raise
    
    def discover_tables_from_toc(self, toc_url):
        db.add_log("INFO", f"Starting table discovery from TOC: {toc_url}")
        
        parsed = urlparse(toc_url)
        if 'oedmf' in toc_url.lower():
            module = 'Financials'
        elif 'oedmh' in toc_url.lower():
            module = 'HCM'
        else:
            module = 'Unknown'
        
        db.add_log("INFO", f"Fetching TOC: {toc_url}")
        response = requests.get(toc_url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        content = response.text
        
        soup = BeautifulSoup(content, 'lxml')
        
        table_links = []
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            if self._is_table_link(href, text):
                full_url = urljoin(toc_url, href)
                table_name = self._extract_table_name_from_link(text, href)
                table_links.append({
                    'url': full_url,
                    'name': table_name,
                    'module': module
                })
        
        unique_tables = {}
        for t in table_links:
            if t['url'] not in unique_tables:
                unique_tables[t['url']] = t
        
        table_links = list(unique_tables.values())
        
        added_count = 0
        for table in table_links:
            if db.add_discovered_table(table['url'], table['name'], table['module']):
                added_count += 1
        
        db.add_log("INFO", f"Discovered {len(table_links)} unique tables, added {added_count} new tables to queue")
        return len(table_links)
    
    def _is_table_link(self, href, text):
        href_lower = href.lower()
        
        if re.search(r'^[a-z][a-z0-9_]*-\d+\.html?(#.*)?$', href_lower):
            return True
        
        table_patterns = [
            r'[a-z][a-z0-9]*-\d+\.html',
            r'table.*\.htm',
            r'.*_all\.htm',
            r'.*_b\.htm',
            r'.*_tl\.htm',
            r'.*_v\.htm',
            r'.*_vl\.htm',
        ]
        
        for pattern in table_patterns:
            if re.search(pattern, href_lower):
                return True
        
        return False
    
    def _extract_table_name_from_link(self, text, href):
        clean_href = href.split('#')[0]
        
        match = re.search(r'^([a-z][a-z0-9_]*)-\d+\.html?$', clean_href, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        
        match = re.search(r'([a-z][a-z0-9_]*)-\d+\.html', clean_href, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        
        text = text.strip()
        if text:
            text = re.sub(r'\s+', '_', text)
            text = re.sub(r'[^A-Za-z0-9_]', '', text)
            if text:
                return text.upper()[:100]
        
        match = re.search(r'([A-Za-z_][A-Za-z0-9_]*)\.htm', clean_href)
        if match:
            return match.group(1).upper()
        return "UNKNOWN_TABLE"
    
    def parse_table_page(self, html_content, source_url):
        soup = BeautifulSoup(html_content, 'lxml')
        
        result = {
            'table_name': None,
            'object_type': 'TABLE',
            'schema_name': None,
            'description': None,
            'columns': [],
            'primary_keys': [],
            'foreign_keys': [],
            'indexes': []
        }
        
        title = soup.find('title')
        if title:
            title_text = title.get_text(strip=True)
            match = re.search(r'([A-Z][A-Z0-9_]+)', title_text)
            if match:
                result['table_name'] = match.group(1)
        
        h1 = soup.find('h1')
        if h1:
            h1_text = h1.get_text(strip=True)
            match = re.search(r'([A-Z][A-Z0-9_]+)', h1_text)
            if match:
                result['table_name'] = match.group(1)
        
        page_text = soup.get_text().lower()
        if ' view ' in page_text or 'this view' in page_text or 'view definition' in page_text:
            result['object_type'] = 'VIEW'
        elif '_v ' in (result.get('table_name') or '').lower() or result.get('table_name', '').endswith('_V'):
            result['object_type'] = 'VIEW'
        elif '_vl' in (result.get('table_name') or '').lower() or result.get('table_name', '').endswith('_VL'):
            result['object_type'] = 'VIEW'
        
        for para in soup.find_all('p'):
            text = para.get_text(strip=True)
            if len(text) > 50 and len(text) < 2000:
                if not any(x in text.lower() for x in ['click here', 'copyright', 'oracle']):
                    result['description'] = text[:1000]
                    break
        
        for section in soup.find_all('section', class_='section'):
            section_title = section.find('h2')
            if not section_title:
                continue
            title_text = section_title.get_text(strip=True).lower()
            
            table = section.find('table')
            if not table:
                continue
            
            rows = table.find_all('tr')[1:]
            
            if 'column' in title_text and 'foreign' not in title_text:
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        col_name = cells[0].get_text(strip=True)[:100]
                        if col_name and col_name.upper() not in ['COLUMN', 'COLUMN NAME', 'NAME']:
                            col_data = {
                                'column_name': col_name,
                                'data_type': cells[1].get_text(strip=True)[:50] if len(cells) > 1 else '',
                                'nullable': cells[2].get_text(strip=True)[:10] if len(cells) > 2 else 'Y',
                                'description': cells[3].get_text(strip=True)[:500] if len(cells) > 3 else ''
                            }
                            result['columns'].append(col_data)
            
            elif 'primary key' in title_text:
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        pk_name = cells[0].get_text(strip=True)
                        pk_cols = cells[1].get_text(strip=True)
                        if pk_cols and pk_cols.upper() not in ['COLUMNS', 'COLUMN']:
                            for col in pk_cols.split(','):
                                col = col.strip()
                                if col:
                                    result['primary_keys'].append(col)
            
            elif 'foreign key' in title_text:
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        table_name = cells[0].get_text(strip=True)[:100]
                        foreign_table = cells[1].get_text(strip=True)[:100] if len(cells) > 1 else ''
                        fk_column = cells[2].get_text(strip=True)[:100] if len(cells) > 2 else ''
                        if table_name and table_name.upper() not in ['TABLE', 'FOREIGN KEY']:
                            fk_data = {
                                'column_name': fk_column,
                                'ref_table': foreign_table,
                                'ref_column': fk_column
                            }
                            result['foreign_keys'].append(fk_data)
            
            elif 'index' in title_text:
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        idx_name = cells[0].get_text(strip=True)[:100]
                        idx_cols = cells[1].get_text(strip=True)[:200] if len(cells) > 1 else ''
                        if idx_name and idx_name.upper() not in ['INDEX', 'INDEX NAME', 'NAME']:
                            idx_data = {
                                'index_name': idx_name,
                                'column_name': idx_cols
                            }
                            result['indexes'].append(idx_data)
        
        return result
    
    def ingest_single_table(self, table_info):
        table_url = table_info['table_url']
        module = table_info.get('module', 'Unknown')
        
        try:
            db.update_table_status(table_url, 'IN_PROGRESS')
            db.add_log("INFO", f"Processing table: {table_url}")
            
            content = self.fetch_page(table_url)
            parsed = self.parse_table_page(content, table_url)
            
            table_name = parsed['table_name'] or table_info.get('table_name', 'UNKNOWN')
            
            db.save_table_metadata(
                table_name=table_name,
                module=module,
                schema_name=parsed.get('schema_name'),
                description=parsed.get('description'),
                source_url=table_url,
                object_type=parsed.get('object_type', 'TABLE')
            )
            
            for col in parsed['columns']:
                db.save_column(
                    table_name=table_name,
                    column_name=col['column_name'],
                    data_type=col.get('data_type'),
                    nullable=col.get('nullable'),
                    description=col.get('description')
                )
            
            for pk in parsed['primary_keys']:
                db.save_primary_key(table_name, pk)
            
            for fk in parsed['foreign_keys']:
                db.save_foreign_key(
                    table_name=table_name,
                    column_name=fk['column_name'],
                    ref_table=fk.get('ref_table'),
                    ref_column=fk.get('ref_column')
                )
            
            for idx in parsed['indexes']:
                db.save_index(
                    table_name=table_name,
                    index_name=idx['index_name'],
                    column_name=idx.get('column_name')
                )
            
            db.update_table_status(table_url, 'COMPLETED')
            db.add_log("INFO", f"Completed: {table_name} - {len(parsed['columns'])} columns")
            
            return True
            
        except Exception as e:
            error_msg = str(e)[:500]
            db.update_table_status(table_url, 'FAILED', error_msg)
            db.add_log("ERROR", f"Failed to ingest {table_url}: {error_msg}")
            return False
    
    def run_ingestion(self):
        self.reset_stop()
        db.set_ingestion_state(is_running=True, is_paused=False)
        db.reset_in_progress_to_pending()
        
        try:
            self.start_browser()
            
            while not self._should_stop:
                state = db.get_ingestion_state()
                if state.get('is_paused'):
                    db.add_log("INFO", "Ingestion paused")
                    break
                
                next_table = db.get_next_pending_table()
                if not next_table:
                    db.add_log("INFO", "No more pending tables - ingestion complete")
                    break
                
                self.ingest_single_table(next_table)
                
                if not self._should_stop:
                    self.polite_delay()
            
        except Exception as e:
            db.add_log("ERROR", f"Ingestion error: {str(e)}")
        finally:
            self.stop_browser()
            if not db.get_ingestion_state().get('is_paused'):
                db.set_ingestion_state(is_running=False)

scraper_instance = FusionScraper()
