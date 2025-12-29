"""
Fusion DB Schema - Database Module
Handles PostgreSQL database operations for storing Oracle Fusion schema metadata.
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime
from contextlib import contextmanager

DATABASE_URL = os.environ.get('NEON_DATABASE_URL') or os.environ.get('DATABASE_URL')

@contextmanager
def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()

def init_database():
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fusion_ingestion_state (
                id SERIAL PRIMARY KEY,
                table_url TEXT UNIQUE NOT NULL,
                table_name TEXT,
                module TEXT,
                status TEXT DEFAULT 'PENDING',
                last_attempt_at TIMESTAMP,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fusion_tables (
                id SERIAL PRIMARY KEY,
                table_name TEXT UNIQUE NOT NULL,
                object_type TEXT DEFAULT 'TABLE',
                module TEXT,
                schema_name TEXT,
                description TEXT,
                doc_version TEXT DEFAULT '26A',
                source_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fusion_columns (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                data_type TEXT,
                nullable TEXT,
                description TEXT,
                UNIQUE(table_name, column_name)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fusion_primary_keys (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                UNIQUE(table_name, column_name)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fusion_foreign_keys (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                ref_table TEXT,
                ref_column TEXT,
                UNIQUE(table_name, column_name, ref_table, ref_column)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fusion_indexes (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                index_name TEXT NOT NULL,
                column_name TEXT,
                UNIQUE(table_name, index_name, column_name)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fusion_ingestion_logs (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                level TEXT,
                message TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ingestion_config (
                id SERIAL PRIMARY KEY,
                toc_url TEXT,
                is_running INTEGER DEFAULT 0,
                is_paused INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
    return True

def add_log(level, message):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO fusion_ingestion_logs (timestamp, level, message)
            VALUES (%s, %s, %s)
        ''', (datetime.now(), level, message))
        conn.commit()

def get_logs(limit=100):
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('''
            SELECT timestamp, level, message FROM fusion_ingestion_logs
            ORDER BY id DESC LIMIT %s
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]

def add_discovered_table(table_url, table_name, module):
    with get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO fusion_ingestion_state (table_url, table_name, module, status)
                VALUES (%s, %s, %s, 'PENDING')
                ON CONFLICT (table_url) DO NOTHING
            ''', (table_url, table_name, module))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            return False

def get_pending_tables():
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('''
            SELECT * FROM fusion_ingestion_state WHERE status = 'PENDING'
            ORDER BY id
        ''')
        return [dict(row) for row in cursor.fetchall()]

def get_next_pending_table():
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('''
            SELECT * FROM fusion_ingestion_state WHERE status = 'PENDING'
            ORDER BY id LIMIT 1
        ''')
        row = cursor.fetchone()
        return dict(row) if row else None

def update_table_status(table_url, status, error_message=None):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE fusion_ingestion_state 
            SET status = %s, last_attempt_at = %s, error_message = %s
            WHERE table_url = %s
        ''', (status, datetime.now(), error_message, table_url))
        conn.commit()

def save_table_metadata(table_name, module, schema_name, description, source_url, object_type='TABLE'):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO fusion_tables (table_name, object_type, module, schema_name, description, source_url)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (table_name) DO UPDATE SET
                object_type = EXCLUDED.object_type,
                module = EXCLUDED.module,
                schema_name = EXCLUDED.schema_name,
                description = EXCLUDED.description,
                source_url = EXCLUDED.source_url
        ''', (table_name, object_type, module, schema_name, description, source_url))
        conn.commit()

def save_column(table_name, column_name, data_type, nullable, description):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO fusion_columns (table_name, column_name, data_type, nullable, description)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (table_name, column_name) DO UPDATE SET
                data_type = EXCLUDED.data_type,
                nullable = EXCLUDED.nullable,
                description = EXCLUDED.description
        ''', (table_name, column_name, data_type, nullable, description))
        conn.commit()

def save_primary_key(table_name, column_name):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO fusion_primary_keys (table_name, column_name)
            VALUES (%s, %s)
            ON CONFLICT (table_name, column_name) DO NOTHING
        ''', (table_name, column_name))
        conn.commit()

def save_foreign_key(table_name, column_name, ref_table, ref_column):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO fusion_foreign_keys (table_name, column_name, ref_table, ref_column)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (table_name, column_name, ref_table, ref_column) DO NOTHING
        ''', (table_name, column_name, ref_table, ref_column))
        conn.commit()

def save_index(table_name, index_name, column_name):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO fusion_indexes (table_name, index_name, column_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (table_name, index_name, column_name) DO NOTHING
        ''', (table_name, index_name, column_name))
        conn.commit()

def get_summary():
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('SELECT COUNT(*) as count FROM fusion_tables')
        tables_count = cursor.fetchone()['count']
        
        cursor.execute('SELECT COUNT(*) as count FROM fusion_columns')
        columns_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM fusion_ingestion_state WHERE status = 'PENDING'")
        pending_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM fusion_ingestion_state WHERE status = 'COMPLETED'")
        completed_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM fusion_ingestion_state WHERE status = 'FAILED'")
        failed_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM fusion_ingestion_state WHERE status = 'IN_PROGRESS'")
        in_progress_count = cursor.fetchone()['count']
        
        cursor.execute('SELECT COUNT(*) as count FROM fusion_ingestion_state')
        total_discovered = cursor.fetchone()['count']
        
        cursor.execute('SELECT table_name FROM fusion_tables ORDER BY id DESC LIMIT 1')
        last_table_row = cursor.fetchone()
        last_table = last_table_row['table_name'] if last_table_row else None
        
        return {
            'tables_ingested': tables_count,
            'columns_ingested': columns_count,
            'pending': pending_count,
            'completed': completed_count,
            'failed': failed_count,
            'in_progress': in_progress_count,
            'total_discovered': total_discovered,
            'last_processed_table': last_table
        }

def get_ingestion_state():
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT * FROM ingestion_config ORDER BY id DESC LIMIT 1')
        row = cursor.fetchone()
        if row:
            return {'is_running': bool(row['is_running']), 'is_paused': bool(row['is_paused']), 'toc_url': row['toc_url']}
        return {'is_running': False, 'is_paused': False, 'toc_url': None}

def set_ingestion_state(is_running=None, is_paused=None, toc_url=None):
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT id FROM ingestion_config ORDER BY id DESC LIMIT 1')
        row = cursor.fetchone()
        
        if row:
            updates = []
            values = []
            if is_running is not None:
                updates.append('is_running = %s')
                values.append(1 if is_running else 0)
            if is_paused is not None:
                updates.append('is_paused = %s')
                values.append(1 if is_paused else 0)
            if toc_url is not None:
                updates.append('toc_url = %s')
                values.append(toc_url)
            if updates:
                values.append(row['id'])
                cursor.execute(f"UPDATE ingestion_config SET {', '.join(updates)} WHERE id = %s", values)
        else:
            cursor.execute('''
                INSERT INTO ingestion_config (toc_url, is_running, is_paused)
                VALUES (%s, %s, %s)
            ''', (toc_url or '', 1 if is_running else 0, 1 if is_paused else 0))
        conn.commit()

def reset_in_progress_to_pending():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE fusion_ingestion_state SET status = 'PENDING' WHERE status = 'IN_PROGRESS'
        ''')
        conn.commit()

def clear_all_data():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM fusion_ingestion_state')
        cursor.execute('DELETE FROM fusion_tables')
        cursor.execute('DELETE FROM fusion_columns')
        cursor.execute('DELETE FROM fusion_primary_keys')
        cursor.execute('DELETE FROM fusion_foreign_keys')
        cursor.execute('DELETE FROM fusion_indexes')
        cursor.execute('DELETE FROM fusion_ingestion_logs')
        cursor.execute('DELETE FROM ingestion_config')
        conn.commit()

def execute_query(query):
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        if query.strip().upper().startswith('SELECT'):
            return [dict(row) for row in cursor.fetchall()]
        else:
            conn.commit()
            return [{'affected_rows': cursor.rowcount}]

init_database()
