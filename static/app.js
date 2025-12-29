let statusInterval = null;
let logsInterval = null;

document.addEventListener('DOMContentLoaded', () => {
    initEventListeners();
    startPolling();
    fetchStatus();
    fetchLogs();
});

function initEventListeners() {
    document.getElementById('discoverBtn').addEventListener('click', discoverTables);
    document.getElementById('startBtn').addEventListener('click', startIngestion);
    document.getElementById('pauseBtn').addEventListener('click', pauseIngestion);
    document.getElementById('resumeBtn').addEventListener('click', resumeIngestion);
    document.getElementById('retryBtn').addEventListener('click', retryFailed);
    document.getElementById('resetBtn').addEventListener('click', resetAll);
    document.getElementById('searchTableBtn').addEventListener('click', searchTable);
    document.getElementById('ingestTableBtn').addEventListener('click', ingestTableByName);
    document.getElementById('tableNameInput').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') searchTable();
    });
    document.getElementById('runQueryBtn').addEventListener('click', runQuery);
}

function setUrl(url) {
    document.getElementById('tocUrl').value = url;
}

async function discoverTables() {
    const tocUrl = document.getElementById('tocUrl').value.trim();
    if (!tocUrl) {
        alert('Please enter a TOC URL');
        return;
    }

    const btn = document.getElementById('discoverBtn');
    btn.disabled = true;
    btn.textContent = 'Discovering...';

    try {
        const response = await fetch('/api/discover', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ toc_url: tocUrl })
        });
        const data = await response.json();
        
        if (data.success) {
            alert(data.message);
            fetchStatus();
            fetchLogs();
        } else {
            alert('Error: ' + data.error);
        }
    } catch (error) {
        alert('Failed to discover tables: ' + error.message);
    } finally {
        btn.disabled = false;
        btn.textContent = 'Discover Tables';
    }
}

async function startIngestion() {
    const btn = document.getElementById('startBtn');
    btn.disabled = true;

    try {
        const response = await fetch('/api/start', { method: 'POST' });
        const data = await response.json();
        
        if (data.success) {
            updateButtonStates(true, false);
        } else {
            alert('Error: ' + data.error);
            btn.disabled = false;
        }
    } catch (error) {
        alert('Failed to start: ' + error.message);
        btn.disabled = false;
    }
}

async function pauseIngestion() {
    try {
        const response = await fetch('/api/pause', { method: 'POST' });
        const data = await response.json();
        
        if (data.success) {
            updateButtonStates(false, true);
        }
    } catch (error) {
        alert('Failed to pause: ' + error.message);
    }
}

async function resumeIngestion() {
    try {
        const response = await fetch('/api/resume', { method: 'POST' });
        const data = await response.json();
        
        if (data.success) {
            updateButtonStates(true, false);
        }
    } catch (error) {
        alert('Failed to resume: ' + error.message);
    }
}

async function retryFailed() {
    try {
        const response = await fetch('/api/retry-failed', { method: 'POST' });
        const data = await response.json();
        alert(data.message);
        fetchStatus();
    } catch (error) {
        alert('Failed: ' + error.message);
    }
}

async function resetAll() {
    if (!confirm('Are you sure you want to reset all data? This cannot be undone.')) {
        return;
    }

    try {
        const response = await fetch('/api/reset', { method: 'POST' });
        const data = await response.json();
        alert(data.message);
        fetchStatus();
        fetchLogs();
    } catch (error) {
        alert('Failed: ' + error.message);
    }
}

async function fetchStatus() {
    try {
        const response = await fetch('/api/status');
        const data = await response.json();
        updateUI(data);
    } catch (error) {
        console.error('Status fetch failed:', error);
    }
}

async function fetchLogs() {
    try {
        const response = await fetch('/api/logs?limit=50');
        const data = await response.json();
        updateLogs(data.logs);
    } catch (error) {
        console.error('Logs fetch failed:', error);
    }
}

function updateUI(data) {
    const summary = data.summary || {};
    
    document.getElementById('totalDiscovered').textContent = summary.total_discovered || 0;
    document.getElementById('completedCount').textContent = summary.completed || 0;
    document.getElementById('pendingCount').textContent = summary.pending || 0;
    document.getElementById('failedCount').textContent = summary.failed || 0;
    document.getElementById('tablesIngested').textContent = summary.tables_ingested || 0;
    document.getElementById('columnsIngested').textContent = summary.columns_ingested || 0;
    document.getElementById('lastProcessed').textContent = summary.last_processed_table || '-';
    document.getElementById('currentTable').textContent = data.current_table || '-';

    const total = summary.total_discovered || 0;
    const completed = summary.completed || 0;
    const progress = total > 0 ? (completed / total * 100) : 0;
    document.getElementById('progressBar').style.width = progress + '%';

    updateStatusIndicator(data.is_running, data.is_paused);
    updateButtonStates(data.is_running, data.is_paused);
    
    const hasPending = (summary.pending || 0) > 0;
    document.getElementById('startBtn').disabled = data.is_running || !hasPending;
}

function updateStatusIndicator(isRunning, isPaused) {
    const dot = document.querySelector('.status-dot');
    const text = document.querySelector('.status-text');
    
    dot.className = 'status-dot';
    
    if (isRunning && !isPaused) {
        dot.classList.add('running');
        text.textContent = 'Running';
    } else if (isPaused) {
        dot.classList.add('paused');
        text.textContent = 'Paused';
    } else {
        dot.classList.add('idle');
        text.textContent = 'Idle';
    }
}

function updateButtonStates(isRunning, isPaused) {
    document.getElementById('pauseBtn').disabled = !isRunning || isPaused;
    document.getElementById('resumeBtn').disabled = !isPaused;
}

function updateLogs(logs) {
    const container = document.getElementById('logContainer');
    
    if (!logs || logs.length === 0) {
        container.innerHTML = '<p class="log-placeholder">Logs will appear here...</p>';
        return;
    }

    container.innerHTML = logs.map(log => {
        const time = log.timestamp ? new Date(log.timestamp).toLocaleTimeString() : '';
        return `
            <div class="log-entry">
                <span class="log-time">${time}</span>
                <span class="log-level ${log.level}">${log.level}</span>
                <span class="log-message">${escapeHtml(log.message)}</span>
            </div>
        `;
    }).join('');
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function startPolling() {
    statusInterval = setInterval(fetchStatus, 3000);
    logsInterval = setInterval(fetchLogs, 5000);
}

async function searchTable() {
    const tableName = document.getElementById('tableNameInput').value.trim();
    if (!tableName) {
        alert('Please enter a table name');
        return;
    }

    const btn = document.getElementById('searchTableBtn');
    btn.disabled = true;
    btn.textContent = 'Searching...';

    try {
        const response = await fetch('/api/search-table', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ table_name: tableName })
        });
        const data = await response.json();
        
        if (data.success) {
            displaySearchResults(data.discovered, data.ingested);
        } else {
            alert('Error: ' + data.error);
        }
    } catch (error) {
        alert('Search failed: ' + error.message);
    } finally {
        btn.disabled = false;
        btn.textContent = 'Search';
    }
}

function displaySearchResults(discovered, ingested) {
    const container = document.getElementById('searchResults');
    
    if ((!discovered || discovered.length === 0) && (!ingested || ingested.length === 0)) {
        container.innerHTML = '<p class="no-results">No tables found. Run discovery first.</p>';
        return;
    }

    let html = '';
    
    if (ingested && ingested.length > 0) {
        html += '<div class="results-section"><h4>Already Ingested:</h4>';
        ingested.forEach(t => {
            html += `<div class="result-item ingested">
                <span class="table-name">${escapeHtml(t.table_name)}</span>
                <span class="status-badge completed">Completed</span>
            </div>`;
        });
        html += '</div>';
    }
    
    if (discovered && discovered.length > 0) {
        html += '<div class="results-section"><h4>Discovered Tables:</h4>';
        discovered.forEach(t => {
            const statusClass = t.status.toLowerCase();
            html += `<div class="result-item" data-url="${escapeHtml(t.table_url)}" data-name="${escapeHtml(t.table_name)}">
                <span class="table-name">${escapeHtml(t.table_name)}</span>
                <span class="status-badge ${statusClass}">${t.status}</span>
                <button class="btn btn-small btn-success" onclick="ingestSingleTable('${escapeHtml(t.table_url)}', '${escapeHtml(t.table_name)}')">Ingest</button>
            </div>`;
        });
        html += '</div>';
    }
    
    container.innerHTML = html;
}

async function ingestSingleTable(tableUrl, tableName) {
    if (!confirm(`Ingest table ${tableName}?`)) return;
    
    try {
        const response = await fetch('/api/ingest-single', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ table_url: tableUrl, table_name: tableName })
        });
        const data = await response.json();
        
        if (data.success) {
            alert(data.message);
            fetchStatus();
            fetchLogs();
            searchTable();
        } else {
            alert('Error: ' + data.error);
        }
    } catch (error) {
        alert('Ingest failed: ' + error.message);
    }
}

async function ingestTableByName() {
    const tableName = document.getElementById('tableNameInput').value.trim();
    if (!tableName) {
        alert('Please enter a table name');
        return;
    }

    const btn = document.getElementById('ingestTableBtn');
    btn.disabled = true;
    btn.textContent = 'Ingesting...';

    try {
        const response = await fetch('/api/ingest-by-name', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ table_name: tableName })
        });
        const data = await response.json();
        
        if (data.success) {
            alert(data.message);
            fetchStatus();
            fetchLogs();
        } else {
            alert('Error: ' + data.error);
        }
    } catch (error) {
        alert('Ingest failed: ' + error.message);
    } finally {
        btn.disabled = false;
        btn.textContent = 'Ingest';
    }
}

function setQuery(sql) {
    document.getElementById('sqlQuery').value = sql;
}

async function runQuery() {
    const sql = document.getElementById('sqlQuery').value.trim();
    if (!sql) {
        alert('Please enter a SQL query');
        return;
    }

    const btn = document.getElementById('runQueryBtn');
    btn.disabled = true;
    btn.textContent = 'Running...';

    try {
        const response = await fetch('/api/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql: sql })
        });
        const data = await response.json();
        
        if (data.success) {
            displayQueryResults(data.columns, data.rows, data.count);
        } else {
            document.getElementById('queryResults').innerHTML = 
                `<div class="query-error">Error: ${escapeHtml(data.error)}</div>`;
        }
    } catch (error) {
        document.getElementById('queryResults').innerHTML = 
            `<div class="query-error">Query failed: ${escapeHtml(error.message)}</div>`;
    } finally {
        btn.disabled = false;
        btn.textContent = 'Run Query';
    }
}

function displayQueryResults(columns, rows, count) {
    const container = document.getElementById('queryResults');
    
    if (!rows || rows.length === 0) {
        container.innerHTML = '<p class="no-results">No results found.</p>';
        return;
    }

    let html = `<p class="result-count">${count} row(s) returned</p>`;
    html += '<div class="table-wrapper"><table class="result-table"><thead><tr>';
    
    columns.forEach(col => {
        html += `<th>${escapeHtml(col)}</th>`;
    });
    html += '</tr></thead><tbody>';
    
    rows.forEach(row => {
        html += '<tr>';
        columns.forEach(col => {
            const val = row[col] !== null ? row[col] : '';
            html += `<td>${escapeHtml(String(val))}</td>`;
        });
        html += '</tr>';
    });
    
    html += '</tbody></table></div>';
    container.innerHTML = html;
}
