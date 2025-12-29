"""
Fusion DB Schema - Main Flask Application
A safe, polite ingestion platform for Oracle Fusion Cloud documentation.
Powers Text-to-SQL / NL2SQL agents with accurate database schema.
"""
import os
import threading
from flask import Flask, render_template, jsonify, request

import database as db
from scraper import scraper_instance

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "fusion-db-schema-secret-key")

ingestion_thread = None
ingestion_lock = threading.Lock()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/discover', methods=['POST'])
def discover_tables():
    data = request.get_json()
    toc_url = data.get('toc_url', '').strip()
    
    if not toc_url:
        return jsonify({'success': False, 'error': 'Please provide a TOC URL'}), 400
    
    if not toc_url.startswith('https://docs.oracle.com'):
        return jsonify({'success': False, 'error': 'URL must be from docs.oracle.com'}), 400
    
    try:
        db.set_ingestion_state(toc_url=toc_url)
        count = scraper_instance.discover_tables_from_toc(toc_url)
        return jsonify({
            'success': True,
            'message': f'Discovered {count} tables',
            'count': count
        })
    except Exception as e:
        db.add_log("ERROR", f"Discovery failed: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/start', methods=['POST'])
def start_ingestion():
    global ingestion_thread
    
    with ingestion_lock:
        state = db.get_ingestion_state()
        if state.get('is_running') and not state.get('is_paused'):
            return jsonify({'success': False, 'error': 'Ingestion already running'}), 400
        
        pending = db.get_pending_tables()
        if not pending:
            return jsonify({'success': False, 'error': 'No pending tables. Run discovery first.'}), 400
        
        db.add_log("INFO", "Starting ingestion process")
        
        ingestion_thread = threading.Thread(target=scraper_instance.run_ingestion, daemon=True)
        ingestion_thread.start()
        
        return jsonify({'success': True, 'message': 'Ingestion started'})

@app.route('/api/pause', methods=['POST'])
def pause_ingestion():
    db.add_log("INFO", "Pause requested by user")
    db.set_ingestion_state(is_paused=True)
    scraper_instance.request_stop()
    return jsonify({'success': True, 'message': 'Pause signal sent'})

@app.route('/api/resume', methods=['POST'])
def resume_ingestion():
    global ingestion_thread
    
    with ingestion_lock:
        db.add_log("INFO", "Resume requested by user")
        db.set_ingestion_state(is_paused=False)
        
        ingestion_thread = threading.Thread(target=scraper_instance.run_ingestion, daemon=True)
        ingestion_thread.start()
        
        return jsonify({'success': True, 'message': 'Ingestion resumed'})

@app.route('/api/status', methods=['GET'])
def get_status():
    state = db.get_ingestion_state()
    summary = db.get_summary()
    
    current_table = None
    with db.get_connection() as conn:
        from psycopg2.extras import RealDictCursor
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT table_name FROM fusion_ingestion_state WHERE status = 'IN_PROGRESS' LIMIT 1")
        row = cursor.fetchone()
        if row:
            current_table = row['table_name']
    
    return jsonify({
        'is_running': state.get('is_running', False),
        'is_paused': state.get('is_paused', False),
        'current_table': current_table,
        'summary': summary
    })

@app.route('/api/logs', methods=['GET'])
def get_logs():
    limit = request.args.get('limit', 50, type=int)
    logs = db.get_logs(limit)
    return jsonify({'logs': logs})

@app.route('/api/reset', methods=['POST'])
def reset_all():
    db.add_log("WARN", "Full reset requested by user")
    scraper_instance.request_stop()
    db.clear_all_data()
    db.init_database()
    return jsonify({'success': True, 'message': 'All data cleared'})

@app.route('/api/retry-failed', methods=['POST'])
def retry_failed():
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE fusion_ingestion_state SET status = 'PENDING' WHERE status = 'FAILED'")
        count = cursor.rowcount
        conn.commit()
    
    db.add_log("INFO", f"Marked {count} failed tables for retry")
    return jsonify({'success': True, 'message': f'Marked {count} failed tables for retry'})

@app.route('/api/search-table', methods=['POST'])
def search_table():
    data = request.get_json()
    table_name = data.get('table_name', '').strip().upper()
    
    if not table_name:
        return jsonify({'success': False, 'error': 'Please provide a table name'}), 400
    
    with db.get_connection() as conn:
        from psycopg2.extras import RealDictCursor
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('''
            SELECT * FROM fusion_ingestion_state 
            WHERE UPPER(table_name) LIKE %s 
            ORDER BY table_name LIMIT 20
        ''', (f'%{table_name}%',))
        results = [dict(row) for row in cursor.fetchall()]
        
        cursor.execute('''
            SELECT * FROM fusion_tables 
            WHERE UPPER(table_name) LIKE %s
            ORDER BY table_name LIMIT 20
        ''', (f'%{table_name}%',))
        ingested = [dict(row) for row in cursor.fetchall()]
    
    return jsonify({
        'success': True,
        'discovered': results,
        'ingested': ingested
    })

@app.route('/api/ingest-single', methods=['POST'])
def ingest_single():
    data = request.get_json()
    table_url = data.get('table_url', '').strip()
    table_name = data.get('table_name', '').strip()
    
    if not table_url:
        return jsonify({'success': False, 'error': 'Please provide a table URL'}), 400
    
    try:
        db.add_log("INFO", f"Single table ingest requested: {table_name}")
        
        table_info = {
            'table_url': table_url,
            'table_name': table_name,
            'module': 'OnDemand'
        }
        
        success = scraper_instance.ingest_single_table(table_info)
        
        if success:
            return jsonify({'success': True, 'message': f'Successfully ingested {table_name}'})
        else:
            return jsonify({'success': False, 'error': f'Failed to ingest {table_name}'}), 500
    except Exception as e:
        db.add_log("ERROR", f"Single ingest failed: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/ingest-by-name', methods=['POST'])
def ingest_by_name():
    data = request.get_json()
    table_name = data.get('table_name', '').strip().upper()
    base_url = data.get('base_url', 'https://docs.oracle.com/en/cloud/saas/financials/26a/oedmf/')
    
    if not table_name:
        return jsonify({'success': False, 'error': 'Please provide a table name'}), 400
    
    try:
        db.add_log("INFO", f"Searching for table: {table_name}")
        
        with db.get_connection() as conn:
            from psycopg2.extras import RealDictCursor
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute('''
                SELECT table_url, table_name FROM fusion_ingestion_state 
                WHERE UPPER(table_name) LIKE %s LIMIT 1
            ''', (f'%{table_name}%',))
            row = cursor.fetchone()
        
        if row:
            table_info = {
                'table_url': row['table_url'],
                'table_name': row['table_name'],
                'module': 'OnDemand'
            }
            success = scraper_instance.ingest_single_table(table_info)
            if success:
                return jsonify({'success': True, 'message': f'Successfully ingested {row["table_name"]}'})
            else:
                return jsonify({'success': False, 'error': f'Failed to ingest {row["table_name"]}'}), 500
        else:
            return jsonify({'success': False, 'error': f'Table {table_name} not found in discovered tables. Run discovery first.'}), 404
            
    except Exception as e:
        db.add_log("ERROR", f"Ingest by name failed: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/query', methods=['POST'])
def run_query():
    data = request.get_json()
    sql = data.get('sql', '').strip()
    
    if not sql:
        return jsonify({'success': False, 'error': 'Please provide a SQL query'}), 400
    
    sql_lower = sql.lower()
    if any(word in sql_lower for word in ['drop', 'delete', 'update', 'insert', 'alter', 'create']):
        return jsonify({'success': False, 'error': 'Only SELECT queries are allowed'}), 400
    
    try:
        with db.get_connection() as conn:
            from psycopg2.extras import RealDictCursor
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            results = [dict(row) for row in rows]
        
        return jsonify({
            'success': True,
            'columns': columns,
            'rows': results,
            'count': len(results)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

if __name__ == '__main__':
    db.init_database()
    app.run(host='0.0.0.0', port=5000, debug=False)
