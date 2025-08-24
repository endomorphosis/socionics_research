import './scraper-client.js';
import { knnSearch } from './knn-search.js';
import { duckdbLoader } from './duckdb-loader.js';

// Global state
let currentData = [];
let filteredData = [];
let currentPage = 1;
let itemsPerPage = 50;
let isCardView = false;
let lastQueryRows = [];

// DOM elements
const elements = {
    navBtns: document.querySelectorAll('.nav-btn'),
    panels: document.querySelectorAll('.panel'),
    searchInput: document.getElementById('search-input'),
    searchBtn: document.getElementById('search-btn'),
    clearSearchBtn: document.getElementById('clear-search-btn'),
    mbtiFilter: document.getElementById('mbti-filter'),
    socionicsFilter: document.getElementById('socionics-filter'),
    dataSourceFilter: document.getElementById('data-source-filter'),
    resultsCount: document.getElementById('results-count'),
    dataStats: document.getElementById('data-stats'),
    tableViewBtn: document.getElementById('table-view-btn'),
    cardViewBtn: document.getElementById('card-view-btn'),
    resultsTable: document.getElementById('results-table'),
    resultsCards: document.getElementById('results-cards'),
    dataTable: document.getElementById('data-table'),
    cardsContainer: document.getElementById('cards-container'),
    pagination: document.getElementById('pagination'),
    loading: document.getElementById('loading'),
    toast: document.getElementById('toast')
};

// Initialize the application
async function init() {
    try {
        showLoading(true);
    await loadSampleData();
        setupEventListeners();
        showPanel('search-panel');
        showToast('Personality Database Viewer initialized successfully', 'success');
    } catch (error) {
        console.error('Initialization failed:', error);
        showToast('Failed to initialize application: ' + error.message, 'error');
    } finally {
        showLoading(false);
    }
}

// Load sample data for demonstration
async function loadSampleData() {
    try {
        // Check if we have actual data files
        const hasData = await checkDataFiles();
        
        if (hasData) {
            // Load real data from parquet files via API
            await loadParquetData();
        } else {
            // Create sample data for demonstration
            currentData = [
                {
                    cid: '1',
                    name: 'Sherlock Holmes',
                    mbti: 'INTJ',
                    socionics: 'ILI',
                    description: 'Brilliant detective with exceptional deductive reasoning',
                    category: 'Fictional Character'
                },
                {
                    cid: '2', 
                    name: 'Tony Stark',
                    mbti: 'ENTP',
                    socionics: 'ILE',
                    description: 'Genius inventor and Iron Man superhero',
                    category: 'Marvel Characters'
                },
                {
                    cid: '3',
                    name: 'Hermione Granger',
                    mbti: 'ISTJ',
                    socionics: 'LSI',
                    description: 'Brilliant witch and loyal friend from Harry Potter',
                    category: 'Harry Potter Characters'
                }
            ];
            
            elements.dataStats.textContent = 'Sample data loaded (3 profiles) - Ready for scraping real data';
        }
        
    filteredData = dedupeByCid(currentData);
        
        // Populate filter dropdowns
        populateFilters();
        
        // Render initial results
        renderResults();

    } catch (error) {
        console.error('Failed to load data:', error);
        elements.dataStats.textContent = 'Error loading data: ' + error.message;
        filteredData = [];
        renderResults();
    }
}

// Load actual data from parquet files using server API
async function loadParquetData() {
    try {
        console.log('Loading parquet data via API...');
        
        const response = await fetch('/api/data/profiles');
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        if (data.error) {
            throw new Error(data.error);
        }
        
        currentData = data.profiles || [];

        // Attempt to load vectors through API, else fallback to none
        knnSearch.setProfiles(currentData);
        let hasVectors = await knnSearch.loadVectors();

        // Initialize DuckDB client for Query Builder
        try {
            await duckdbLoader.init();
            // Load profiles parquet into DuckDB for SQL queries
            let ok = false;
            for (const p of ['/dataset/pdb_profiles.parquet', '/dataset/pdb_profiles_normalized.parquet', '/dataset/pdb_profiles_merged.parquet']) {
                const meta = await duckdbLoader.loadParquetFile(p, 'profiles');
                if (meta?.success) {
                    ok = true;
                    try {
                        const status = document.getElementById('dataset-status');
                        if (status) status.textContent = `Dataset: ${meta.filePath} (${meta.rowCount} rows)`;
                    } catch {}
                    break;
                }
            }
            if (!ok) console.warn('DuckDB could not load any profiles parquet (main/normalized/merged).');
        } catch (e) {
            console.warn('DuckDB client init failed:', e?.message || e);
        }

        elements.dataStats.textContent = `Loaded ${data.count || currentData.length} profiles from parquet files${hasVectors ? ' (KNN enabled)' : ''}`;
        
        console.log(`Successfully loaded ${currentData.length} profiles${hasVectors ? ' with KNN search capabilities' : ''}`);
        
    } catch (error) {
        console.error('Failed to load parquet data:', error);
        throw error;
    }
}

// Check if actual data files exist
async function checkDataFiles() {
    try {
        // Check for the parquet files in the bot_store directory
        const response = await fetch('/dataset');
        if (!response.ok) return false;
        const json = await response.json();
        const files = json?.files || [];
        return files.includes('pdb_profiles.parquet') || files.includes('pdb_profiles_normalized.parquet');
    } catch (error) {
        console.warn('Cannot access parquet files:', error);
        return false;
    }
}

// Populate filter dropdowns
function populateFilters() {
    try {
        // Extract unique MBTI types
        const mbtiTypes = [...new Set(currentData.map(item => item.mbti).filter(Boolean))].sort();
        populateSelect(elements.mbtiFilter, mbtiTypes);

        // Extract unique Socionics types
        const socionicsTypes = [...new Set(currentData.map(item => item.socionics).filter(Boolean))].sort();
        populateSelect(elements.socionicsFilter, socionicsTypes);

    } catch (error) {
        console.warn('Could not populate filters:', error);
    }
}

// Dedupe helper by cid (prefers first occurrence)
function dedupeByCid(arr) {
    const seen = new Set();
    const out = [];
    for (const item of arr) {
        const cid = item && item.cid;
        if (!cid) { out.push(item); continue; }
        if (seen.has(cid)) continue;
        seen.add(cid);
        out.push(item);
    }
    return out;
}

// Populate select element with options
function populateSelect(selectElement, options) {
    // Keep the first default option
    const defaultOption = selectElement.children[0];
    selectElement.innerHTML = '';
    selectElement.appendChild(defaultOption);
    
    options.forEach(option => {
        const optElement = document.createElement('option');
        optElement.value = option;
        optElement.textContent = option;
        selectElement.appendChild(optElement);
    });
}

// Setup event listeners
function setupEventListeners() {
    // Navigation
    elements.navBtns.forEach(btn => {
        btn.addEventListener('click', (e) => {
            const panelId = e.target.id.replace('nav-', '') + '-panel';
            showPanel(panelId);
            
            // Update active nav button
            elements.navBtns.forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
        });
    });

    // Search functionality
    elements.searchBtn.addEventListener('click', performSearch);
    elements.searchInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') performSearch();
    });
    elements.clearSearchBtn.addEventListener('click', clearSearch);

    // Filters
    [elements.mbtiFilter, elements.socionicsFilter, elements.dataSourceFilter].forEach(filter => {
        filter.addEventListener('change', applyFilters);
    });

    // View toggle
    elements.tableViewBtn.addEventListener('click', () => switchView(false));
    elements.cardViewBtn.addEventListener('click', () => switchView(true));

    // Edit panel event listeners
    setupEditPanelListeners();

    // Scraper panel event listeners
    setupScraperPanelListeners();

    // Query panel event listeners
    setupQueryPanelListeners();
}

// Setup edit panel event listeners
function setupEditPanelListeners() {
    const importBtn = document.getElementById('import-btn');
    const exportBtn = document.getElementById('export-btn');
    const backupBtn = document.getElementById('backup-btn');
    const importFile = document.getElementById('import-file');
    
    importBtn.addEventListener('click', () => importFile.click());
    exportBtn.addEventListener('click', exportData);
    backupBtn.addEventListener('click', createBackup);
    importFile.addEventListener('change', handleFileImport);

    // Bulk operations
    document.getElementById('normalize-btn').addEventListener('click', normalizeData);
    document.getElementById('dedupe-btn').addEventListener('click', deduplicateData);
    document.getElementById('validate-btn').addEventListener('click', validateData);
    
    // Profile form handling
    const profileForm = document.getElementById('profile-form');
    const cancelEditBtn = document.getElementById('cancel-edit-btn');
    const deleteProfileBtn = document.getElementById('delete-profile-btn');
    
    if (profileForm) {
        profileForm.addEventListener('submit', handleProfileSave);
    }
    if (cancelEditBtn) {
        cancelEditBtn.addEventListener('click', cancelProfileEdit);
    }
    if (deleteProfileBtn) {
        deleteProfileBtn.addEventListener('click', handleProfileDelete);
    }
}

// Setup scraper panel event listeners
function setupScraperPanelListeners() {
    const startFullScrapeBtn = document.getElementById('start-full-scrape-btn');
    const startIncrementalScrapeBtn = document.getElementById('start-incremental-scrape-btn');
    const scrapeSpecificBtn = document.getElementById('scrape-specific-btn');
    const stopScraperBtn = document.getElementById('stop-scraper-btn');
    const scrapeProfileBtn = document.getElementById('scrape-profile-btn');
    const specificProfileInput = document.getElementById('specific-profile-input');

    startFullScrapeBtn.addEventListener('click', () => startScraping('full'));
    startIncrementalScrapeBtn.addEventListener('click', () => startScraping('incremental'));
    scrapeSpecificBtn.addEventListener('click', () => {
        specificProfileInput.style.display = specificProfileInput.style.display === 'none' ? 'block' : 'none';
    });
    stopScraperBtn.addEventListener('click', stopScraping);
    scrapeProfileBtn.addEventListener('click', scrapeSpecificProfile);
}

// Setup query panel event listeners
function setupQueryPanelListeners() {
    const executeQueryBtn = document.getElementById('execute-query-btn');
    const clearQueryBtn = document.getElementById('clear-query-btn');
    const saveQueryBtn = document.getElementById('save-query-btn');
    const useMergedBtn = document.getElementById('use-merged-dataset-btn');
    const reloadDatasetsBtn = document.getElementById('reload-datasets-btn');
    const exportCsvBtn = document.getElementById('export-csv-btn');
    const exportJsonBtn = document.getElementById('export-json-btn');
    const exportParquetBtn = document.getElementById('export-parquet-btn');
    const cleanDatasetsBtn = document.getElementById('clean-datasets-btn');
    const fillMissingBtn = document.getElementById('fill-missing-btn');
    const datasetSelect = document.getElementById('dataset-select');
    const reloadDatasetListBtn = document.getElementById('reload-dataset-list-btn');
    const sqlQuery = document.getElementById('sql-query');
    const templateBtns = document.querySelectorAll('.template-btn');

    executeQueryBtn.addEventListener('click', executeQuery);
    clearQueryBtn.addEventListener('click', () => sqlQuery.value = '');
    saveQueryBtn.addEventListener('click', saveQuery);
    if (reloadDatasetsBtn) reloadDatasetsBtn.addEventListener('click', reloadDatasets);
    if (exportCsvBtn) exportCsvBtn.addEventListener('click', () => exportQueryResults('csv'));
    if (exportJsonBtn) exportJsonBtn.addEventListener('click', () => exportQueryResults('json'));
    if (exportParquetBtn) exportParquetBtn.addEventListener('click', () => exportQueryResults('parquet'));
    if (cleanDatasetsBtn) cleanDatasetsBtn.addEventListener('click', cleanDatasets);
    if (fillMissingBtn) fillMissingBtn.addEventListener('click', fillMissingData);
    if (useMergedBtn) {
        useMergedBtn.addEventListener('click', async () => {
            try {
                showLoading(true);
                await duckdbLoader.init();
                // Create/replace the profiles table from merged parquet
                const meta = await duckdbLoader.loadParquetFile('/dataset/pdb_profiles_merged.parquet', 'profiles');
                if (!meta?.success) throw new Error(meta?.error || 'Failed to load merged parquet');
                showToast(`Switched to merged dataset (${meta.rowCount} rows)`, 'success');
            } catch (e) {
                console.error('Use merged dataset failed:', e);
                showToast('Failed to use merged dataset: ' + e.message, 'error');
            } finally {
                showLoading(false);
            }
        });
    }

    // Dataset selector handling
    if (datasetSelect) {
        // Populate options dynamically from /dataset
        (async () => {
            try {
                const resp = await fetch('/dataset');
                const info = await resp.json();
                const baseOptions = [
                    { value: '/dataset/pdb_profiles.parquet', label: 'Main (pdb_profiles.parquet)' },
                    { value: '/dataset/pdb_profiles_normalized.parquet', label: 'Normalized (pdb_profiles_normalized.parquet)' },
                    { value: '/dataset/pdb_profiles_merged.parquet', label: 'Merged (pdb_profiles_merged.parquet)' }
                ];
                // Start fresh
                datasetSelect.innerHTML = '';
                for (const opt of baseOptions) {
                    const o = document.createElement('option'); o.value = opt.value; o.textContent = opt.label; datasetSelect.appendChild(o);
                }
                const exportsList = Array.isArray(info.exports) ? info.exports : [];
                if (exportsList.length) {
                    // Add an optgroup for Exports
                    const group = document.createElement('optgroup'); group.label = 'Exports';
                    for (const rel of exportsList) {
                        const full = '/dataset/' + rel;
                        const o = document.createElement('option'); o.value = full; o.textContent = rel; group.appendChild(o);
                    }
                    datasetSelect.appendChild(group);
                }
            } catch (e) {
                console.warn('Failed to list datasets:', e);
            }
        })();

        // Initialize from saved choice
        const saved = localStorage.getItem('viewer.dataset.path');
        if (saved) datasetSelect.value = saved;
        datasetSelect.addEventListener('change', async (e) => {
            const path = e.target.value;
            await switchDataset(path);
        });
        // If user had a saved dataset, try to load it on startup
        if (saved) {
            switchDataset(saved).catch(() => {});
        }
        if (reloadDatasetListBtn) reloadDatasetListBtn.addEventListener('click', () => { refreshDatasetSelect().catch(() => {}); });
    }

    templateBtns.forEach(btn => {
        btn.addEventListener('click', (e) => {
            sqlQuery.value = e.target.dataset.query;
        });
    });
}

// Show/hide panels
function showPanel(panelId) {
    elements.panels.forEach(panel => {
        panel.classList.remove('active');
    });
    document.getElementById(panelId).classList.add('active');
}

// Perform search with KNN and bag-of-words fallback
async function performSearch() {
    const query = elements.searchInput.value.trim();
    if (!query) {
        applyFilters();
        return;
    }

    try {
        showLoading(true);
    let searchResults = [];
    let searchMethod = 'none';
        
        // Try KNN search first if available
        if (knnSearch.isAvailable()) {
            // Check if query looks like a profile CID for similarity search
            if (query.startsWith('Qm') && query.length > 20) {
                // CID-based similarity search
                searchResults = knnSearch.findSimilarProfiles(query, 20);
                if (searchResults.length > 0) {
                    searchMethod = 'KNN similarity';
                }
            }
            
            // TODO: Implement vector-based text search when we have text encoding capability
            // For now, fall through to bag-of-words search
        }
        
        // Bag-of-words search
        const queryLower = query.toLowerCase();
        const bowResults = currentData.filter(item => {
            return (item.name && item.name.toLowerCase().includes(queryLower)) ||
                   (item.mbti && item.mbti.toLowerCase().includes(queryLower)) ||
                   (item.socionics && item.socionics.toLowerCase().includes(queryLower)) ||
                   (item.description && item.description.toLowerCase().includes(queryLower)) ||
                   (item.category && item.category.toLowerCase().includes(queryLower)) ||
                   (item.cid && item.cid.includes(query)); // Exact CID match
        });

        // Fuse results: prefer KNN if present; else use BoW
        if (searchResults.length > 0) {
            const knnCids = new Set(searchResults.map(r => r.cid));
            const fused = [...searchResults];
            for (const r of bowResults) if (!knnCids.has(r.cid)) fused.push(r);
            searchResults = fused;
            if (searchMethod === 'none') searchMethod = 'bag-of-words';
        } else {
            searchResults = bowResults;
            searchMethod = 'bag-of-words';
        }
        
    filteredData = dedupeByCid(searchResults);
        currentPage = 1;
        renderResults();
        
        // Update status with search method used
        if (searchMethod !== 'none') {
            elements.dataStats.textContent = `Found ${searchResults.length} results using ${searchMethod} search`;
        }
        
    } catch (error) {
        console.error('Search failed:', error);
        showToast('Search failed: ' + error.message, 'error');
    } finally {
        showLoading(false);
    }
}

// Clear search
function clearSearch() {
    elements.searchInput.value = '';
    elements.mbtiFilter.value = '';
    elements.socionicsFilter.value = '';
    elements.dataSourceFilter.value = '';
    filteredData = dedupeByCid(currentData);
    currentPage = 1;
    renderResults();
}

// Apply filters
async function applyFilters() {
    try {
        filteredData = currentData.filter(item => {
            let matches = true;
            
            if (elements.mbtiFilter.value) {
                matches = matches && item.mbti === elements.mbtiFilter.value;
            }
            
            if (elements.socionicsFilter.value) {
                matches = matches && item.socionics === elements.socionicsFilter.value;
            }
            
            return matches;
        });
        
    filteredData = dedupeByCid(filteredData);
    currentPage = 1;
        renderResults();
        
    } catch (error) {
        console.error('Filter failed:', error);
        showToast('Filter failed: ' + error.message, 'error');
    }
}

// Switch between table and card view
function switchView(cardView) {
    isCardView = cardView;
    
    if (cardView) {
        elements.tableViewBtn.classList.remove('active');
        elements.cardViewBtn.classList.add('active');
        elements.resultsTable.style.display = 'none';
        elements.resultsCards.style.display = 'block';
    } else {
        elements.cardViewBtn.classList.remove('active');
        elements.tableViewBtn.classList.add('active');
        elements.resultsCards.style.display = 'none';
        elements.resultsTable.style.display = 'block';
    }
    
    renderResults();
}

// Render results
function renderResults() {
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const pageData = filteredData.slice(startIndex, endIndex);
    
    elements.resultsCount.textContent = `${filteredData.length.toLocaleString()} results`;
    
    if (isCardView) {
        renderCards(pageData);
    } else {
        renderTable(pageData);
    }
    
    renderPagination();
}

// Render table view
function renderTable(data) {
    const thead = elements.dataTable.querySelector('thead');
    const tbody = elements.dataTable.querySelector('tbody');
    
    // Clear existing content
    thead.innerHTML = '';
    tbody.innerHTML = '';
    
    if (data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="100%">No data to display</td></tr>';
        return;
    }
    
    // Create header
    const headerRow = document.createElement('tr');
    const columns = ['name', 'mbti', 'socionics', 'description'];
    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = col.charAt(0).toUpperCase() + col.slice(1);
        headerRow.appendChild(th);
    });
    
    const actionsHeader = document.createElement('th');
    actionsHeader.textContent = 'Actions';
    headerRow.appendChild(actionsHeader);
    thead.appendChild(headerRow);
    
    // Create rows
    data.forEach(row => {
        const tr = document.createElement('tr');
        
        columns.forEach(col => {
            const td = document.createElement('td');
            let value = row[col] || '';
            if (col === 'description' && value.length > 100) {
                value = value.substring(0, 100) + '...';
            }
            td.textContent = value;
            tr.appendChild(td);
        });
        
        // Actions column
        const actionsTd = document.createElement('td');
        const editBtn = document.createElement('button');
        editBtn.textContent = 'Edit';
        editBtn.onclick = () => editProfile(row);
        actionsTd.appendChild(editBtn);
        tr.appendChild(actionsTd);
        
        tbody.appendChild(tr);
    });
}

// Render card view
function renderCards(data) {
    elements.cardsContainer.innerHTML = '';
    
    if (data.length === 0) {
        elements.cardsContainer.innerHTML = '<div>No data to display</div>';
        return;
    }
    
    data.forEach(item => {
        const card = document.createElement('div');
        card.className = 'profile-card';
        
        card.innerHTML = `
            <h3>${item.name || 'Unknown'}</h3>
            <div class="types">
                ${item.mbti ? `<span class="type-badge">${item.mbti}</span>` : ''}
                ${item.socionics ? `<span class="type-badge">${item.socionics}</span>` : ''}
            </div>
            <p>${(item.description || '').substring(0, 150)}${item.description && item.description.length > 150 ? '...' : ''}</p>
            <div style="margin-top: 1rem;">
                <button onclick="editProfile(${JSON.stringify(item).replace(/"/g, '&quot;')})">Edit</button>
            </div>
        `;
        
        elements.cardsContainer.appendChild(card);
    });
}

// Render pagination
function renderPagination() {
    const totalPages = Math.ceil(filteredData.length / itemsPerPage);
    elements.pagination.innerHTML = '';
    
    if (totalPages <= 1) return;
    
    // Previous button
    if (currentPage > 1) {
        const prevBtn = document.createElement('button');
        prevBtn.textContent = 'Previous';
        prevBtn.onclick = () => {
            currentPage--;
            renderResults();
        };
        elements.pagination.appendChild(prevBtn);
    }
    
    // Page numbers
    const startPage = Math.max(1, currentPage - 2);
    const endPage = Math.min(totalPages, currentPage + 2);
    
    for (let i = startPage; i <= endPage; i++) {
        const pageBtn = document.createElement('button');
        pageBtn.textContent = i;
        pageBtn.onclick = () => {
            currentPage = i;
            renderResults();
        };
        
        if (i === currentPage) {
            pageBtn.style.background = '#48bb78';
        }
        
        elements.pagination.appendChild(pageBtn);
    }
    
    // Next button
    if (currentPage < totalPages) {
        const nextBtn = document.createElement('button');
        nextBtn.textContent = 'Next';
        nextBtn.onclick = () => {
            currentPage++;
            renderResults();
        };
        elements.pagination.appendChild(nextBtn);
    }
}

// Edit profile function (placeholder for now)
window.editProfile = function(profile) {
    showPanel('edit-panel');
    elements.navBtns.forEach(btn => btn.classList.remove('active'));
    document.getElementById('nav-edit').classList.add('active');
    
    // Populate edit form
    document.getElementById('profile-id').value = profile.cid || profile.id || '';
    document.getElementById('profile-name').value = profile.name || '';
    document.getElementById('profile-mbti').value = profile.mbti || '';
    document.getElementById('profile-socionics').value = profile.socionics || '';
    document.getElementById('profile-description').value = profile.description || '';
    
    document.getElementById('edit-form').style.display = 'block';
    showToast('Profile loaded for editing', 'success');
};

// Utility functions
function showLoading(show) {
    elements.loading.style.display = show ? 'flex' : 'none';
}

function showToast(message, type = 'success') {
    elements.toast.textContent = message;
    elements.toast.className = `toast ${type}`;
    elements.toast.classList.add('show');
    
    setTimeout(() => {
        elements.toast.classList.remove('show');
    }, 3000);
}

// Placeholder functions for features to be implemented
async function exportData() {
    try {
        showLoading(true);
        const fileName = prompt('Enter output parquet filename', 'pdb_profiles_merged.parquet') || 'pdb_profiles_merged.parquet';
        const resp = await fetch('/api/data/commit-overlay', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filename: fileName })
        });
        const json = await resp.json();
        if (!resp.ok || !json.success) throw new Error(json.error || 'Commit failed');
        showToast(`Overlay committed: ${json.out_file} (${json.merged_count} rows)`, 'success');
        // Auto-switch DuckDB 'profiles' table to the new merged parquet
        try {
            await duckdbLoader.init();
            const mergedUrl = '/dataset/' + (fileName || 'pdb_profiles_merged.parquet');
            const meta = await duckdbLoader.loadParquetFile(mergedUrl, 'profiles');
            if (meta?.success) {
                showToast(`Query Builder now using merged dataset (${meta.rowCount} rows)`, 'success');
                try { 
                    const select = document.getElementById('dataset-select');
                    if (select) { select.value = mergedUrl; localStorage.setItem('viewer.dataset.path', mergedUrl); }
                    const status = document.getElementById('dataset-status');
                    if (status) status.textContent = `Dataset: ${mergedUrl} (${meta.rowCount} rows)`;
                } catch {}
            }
        } catch (e) {
            console.warn('Auto-switch to merged dataset failed:', e?.message || e);
        }
    } catch (e) {
        console.error('Commit overlay failed:', e);
        showToast('Commit failed: ' + e.message, 'error');
    } finally {
        showLoading(false);
    }
}

// Switch dataset helper
async function switchDataset(path) {
    try {
        showLoading(true);
        await duckdbLoader.init();
        const meta = await duckdbLoader.loadParquetFile(path, 'profiles');
        if (!meta?.success) throw new Error(meta?.error || 'Failed to load dataset');
        localStorage.setItem('viewer.dataset.path', path);
        const status = document.getElementById('dataset-status');
        if (status) status.textContent = `Dataset: ${path} (${meta.rowCount} rows)`;
        showToast(`Switched dataset (${meta.rowCount} rows)`, 'success');
    } catch (e) {
        console.error('Switch dataset failed:', e);
        showToast('Failed to switch dataset: ' + e.message, 'error');
    } finally {
        showLoading(false);
    }
}

async function createBackup() {
    showToast('Backup functionality coming soon', 'info');
}

async function handleFileImport(event) {
    showToast('Import functionality coming soon', 'info');
}

async function normalizeData() {
    showToast('Data normalization coming soon', 'info');
}

async function deduplicateData() {
    try {
        showLoading(true);
        const before = currentData.length;
        currentData = dedupeByCid(currentData);
        const after = currentData.length;
        // Update KNN search data
        knnSearch.setProfiles(currentData);
        // Reapply current filters/search
        applyFilters();
        showToast(`Removed duplicates: ${before - after} entries`, 'success');
    } catch (e) {
        console.error('Deduplication failed:', e);
        showToast('Deduplication failed: ' + e.message, 'error');
    } finally {
        showLoading(false);
    }
}

async function validateData() {
    showToast('Data validation coming soon', 'info');
}

// CRUD Operations for profiles

// Handle profile save (Create/Update)
async function handleProfileSave(event) {
    event.preventDefault();
    
    try {
        showLoading(true);
        
        const formData = {
            cid: document.getElementById('profile-id').value,
            name: document.getElementById('profile-name').value,
            mbti: document.getElementById('profile-mbti').value,
            socionics: document.getElementById('profile-socionics').value,
            description: document.getElementById('profile-description').value
        };
        
        const isUpdate = !!formData.cid;
        const method = isUpdate ? 'PUT' : 'POST';
        const url = isUpdate ? `/api/data/profiles/${formData.cid}` : '/api/data/profiles';
        
        const response = await fetch(url, {
            method: method,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });
        
        const result = await response.json();
        
        if (response.ok) {
            // Update local data
            if (isUpdate) {
                // Find and update existing profile
                const index = currentData.findIndex(p => p.cid === formData.cid);
                if (index !== -1) {
                    currentData[index] = { ...currentData[index], ...formData };
                }
            } else {
                // Add new profile with generated CID
                const newProfile = { ...formData, cid: result.cid };
                currentData.push(newProfile);
                currentData = dedupeByCid(currentData);
            }
            
            // Update KNN search data
            knnSearch.setProfiles(currentData);
            
            // Refresh current view
            applyFilters();
            
            // Hide edit form and show success
            cancelProfileEdit();
            showToast(isUpdate ? 'Profile updated successfully' : 'Profile created successfully', 'success');
        } else {
            throw new Error(result.error || 'Save failed');
        }
        
    } catch (error) {
        console.error('Profile save failed:', error);
        showToast('Failed to save profile: ' + error.message, 'error');
    } finally {
        showLoading(false);
    }
}

// Cancel profile editing
function cancelProfileEdit() {
    document.getElementById('edit-form').style.display = 'none';
    document.getElementById('profile-form').reset();
    showPanel('search-panel');
    elements.navBtns.forEach(btn => btn.classList.remove('active'));
    document.getElementById('nav-search').classList.add('active');
}

// Handle profile deletion
async function handleProfileDelete() {
    const cid = document.getElementById('profile-id').value;
    if (!cid) {
        showToast('No profile selected for deletion', 'error');
        return;
    }
    
    if (!confirm('Are you sure you want to delete this profile? This action cannot be undone.')) {
        return;
    }
    
    try {
        showLoading(true);
        
        const response = await fetch(`/api/data/profiles/${cid}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            // Remove from local data
            currentData = currentData.filter(p => p.cid !== cid);
            
            // Update KNN search data
            knnSearch.setProfiles(currentData);
            
            // Refresh current view
            applyFilters();
            
            // Hide edit form and show success
            cancelProfileEdit();
            showToast('Profile deleted successfully', 'success');
        } else {
            throw new Error(result.error || 'Delete failed');
        }
        
    } catch (error) {
        console.error('Profile deletion failed:', error);
        showToast('Failed to delete profile: ' + error.message, 'error');
    } finally {
        showLoading(false);
    }
}

// Add new profile function
function addNewProfile() {
    showPanel('edit-panel');
    elements.navBtns.forEach(btn => btn.classList.remove('active'));
    document.getElementById('nav-edit').classList.add('active');
    
    // Clear form for new profile
    document.getElementById('profile-form').reset();
    document.getElementById('profile-id').value = ''; // Empty CID means new profile
    
    document.getElementById('edit-form').style.display = 'block';
    showToast('Create new profile', 'info');
}

// Make addNewProfile available globally
window.addNewProfile = addNewProfile;

// CRUD Operations for profiles

async function startScraping(mode) {
    try {
        const config = {
            mode: mode,
            maxPages: parseInt(document.getElementById('scraper-pages').value) || 10,
            delay: parseInt(document.getElementById('scraper-delay').value) || 1000,
            browser: document.getElementById('scraper-browser').value || 'playwright',
            url: document.getElementById('scraper-url').value || 'https://www.personality-database.com'
        };

        showToast(`Starting ${mode} scraping...`, 'info');
        updateScraperStatus('running');
        
        const response = await fetch('/api/scraper/start', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(config)
        });

        if (!response.ok) {
            throw new Error(`Failed to start scraper: ${response.status}`);
        }

        const result = await response.json();
        
        if (result.success) {
            showToast(`Scraper started successfully! Process ID: ${result.processId}`, 'success');
            startProgressMonitoring(result.processId);
        } else {
            throw new Error(result.error || 'Unknown error');
        }
        
    } catch (error) {
        console.error('Error starting scraper:', error);
        showToast('Failed to start scraper: ' + error.message, 'error');
        updateScraperStatus('error');
    }
}

async function stopScraping() {
    try {
        showToast('Stopping scraper...', 'info');
        
        // This would call the API to stop the current scraper process
        // For now, just update the UI
        updateScraperStatus('idle');
        showToast('Scraper stopped', 'success');
        
    } catch (error) {
        console.error('Error stopping scraper:', error);
        showToast('Failed to stop scraper: ' + error.message, 'error');
    }
}

async function scrapeSpecificProfile() {
    let url = document.getElementById('profile-url').value.trim();
    if (!url) {
        showToast('Please enter a profile URL', 'error');
        return;
    }
    // Accept numeric IDs and convert to profile URL
    if (/^\d+$/.test(url)) {
        url = `https://www.personality-database.com/profile/${url}`;
    }
    
    try {
        showToast('Scraping specific profile...', 'info');
        
        const response = await fetch('/api/scraper/profile', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                url: url,
                browser: document.getElementById('scraper-browser').value || 'playwright'
            })
        });

        if (!response.ok) {
            throw new Error(`Failed to scrape profile: ${response.status}`);
        }

        const result = await response.json();
        
        if (result.success) {
            showToast('Profile scraped successfully! Reloading data...', 'success');
            // Reload profiles via API and refresh views
            try {
                const resp = await fetch('/api/data/profiles');
                const data = await resp.json();
                if (!resp.ok || data.error) throw new Error(data.error || `HTTP ${resp.status}`);
                currentData = data.profiles || [];
                filteredData = dedupeByCid(currentData);
                // Refresh KNN vectors to include newly scraped items
                try {
                    knnSearch.setProfiles(currentData);
                    await knnSearch.loadVectors();
                } catch {}
                renderResults();
                // Optionally refresh dataset selector (cleaned or merged outputs may have changed)
                try { await refreshDatasetSelect(); } catch {}
            } catch (e) {
                console.warn('Post-scrape reload failed:', e);
            }
        } else {
            throw new Error(result.error || 'Unknown error');
        }
        
    } catch (error) {
        console.error('Error scraping profile:', error);
        showToast('Failed to scrape profile: ' + error.message, 'error');
    }
}

// Update scraper status UI
function updateScraperStatus(status) {
    const statusElement = document.getElementById('scraper-status');
    const stopButton = document.getElementById('stop-scraper-btn');
    const startButtons = document.querySelectorAll('#start-full-scrape-btn, #start-incremental-scrape-btn');
    
    if (statusElement) {
        statusElement.textContent = status.charAt(0).toUpperCase() + status.slice(1);
        statusElement.className = `status-${status}`;
    }

    if (stopButton && startButtons) {
        if (status === 'running') {
            stopButton.disabled = false;
            startButtons.forEach(btn => btn.disabled = true);
        } else {
            stopButton.disabled = true;
            startButtons.forEach(btn => btn.disabled = false);
        }
    }
}

// Start monitoring progress (placeholder for SSE)
function startProgressMonitoring(processId) {
    // This would set up Server-Sent Events to monitor progress
    console.log(`Starting progress monitoring for process ${processId}`);
    
    // For demo, simulate some progress
    let progress = 0;
    const interval = setInterval(() => {
        progress += Math.random() * 10;
        if (progress >= 100) {
            progress = 100;
            clearInterval(interval);
            updateScraperStatus('idle');
            showToast('Scraping completed!', 'success');
        }
        
        const progressFill = document.getElementById('progress-fill');
        const progressText = document.getElementById('progress-text');
        
        if (progressFill) {
            progressFill.style.width = `${progress}%`;
        }
        
        if (progressText) {
            progressText.textContent = `Progress: ${Math.round(progress)}%`;
        }
        
        // Add some log messages
        addScraperLog(`Processing... ${Math.round(progress)}% complete`);
        
    }, 2000);
}

// Add log to scraper logs
function addScraperLog(message) {
    const logsElement = document.getElementById('scraper-logs');
    if (logsElement) {
        const timestamp = new Date().toLocaleTimeString();
        const logMessage = `[${timestamp}] ${message}`;
        
        if (logsElement.textContent) {
            logsElement.textContent += '\n' + logMessage;
        } else {
            logsElement.textContent = logMessage;
        }
        
        logsElement.scrollTop = logsElement.scrollHeight;
    }
}

function renderQueryResults(arrayRows) {
    const container = document.getElementById('query-results-container');
    lastQueryRows = Array.isArray(arrayRows) ? arrayRows : [];
    if (!arrayRows || arrayRows.length === 0) {
        container.innerHTML = '<p class="no-results">No rows returned</p>';
        return;
    }
    // Dedupe by cid when present
    const seen = new Set();
    const deduped = [];
    for (const r of arrayRows) {
        const cid = r && (r.cid || r.CID || r.Cid);
        if (cid) {
            if (seen.has(cid)) continue;
            seen.add(cid);
        }
        deduped.push(r);
    }
    const cols = Object.keys(deduped[0]);
    const table = document.createElement('table');
    table.className = 'results-table';
    const thead = document.createElement('thead');
    const trh = document.createElement('tr');
    cols.forEach(c => { const th = document.createElement('th'); th.textContent = c; trh.appendChild(th); });
    thead.appendChild(trh);
    const tbody = document.createElement('tbody');
    deduped.forEach(r => {
        const tr = document.createElement('tr');
        cols.forEach(c => { const td = document.createElement('td'); td.textContent = r[c]; tr.appendChild(td); });
        tbody.appendChild(tr);
    });
    table.appendChild(thead); table.appendChild(tbody);
    container.innerHTML = ''; container.appendChild(table);
}

async function executeQuery() {
    const query = document.getElementById('sql-query').value.trim();
    if (!query) {
        showToast('Please enter a SQL query', 'error');
        return;
    }
    
    try {
        showLoading(true);
        // Ensure DuckDB is initialized
        await duckdbLoader.init();
        // Ensure a profiles table exists (load if needed)
        try {
            await duckdbLoader.query('SELECT 1 FROM profiles LIMIT 1');
        } catch {
            const meta = await duckdbLoader.loadParquetFile('/dataset/pdb_profiles.parquet', 'profiles');
            if (!meta?.success) {
                const meta2 = await duckdbLoader.loadParquetFile('/dataset/pdb_profiles_normalized.parquet', 'profiles');
                if (!meta2?.success) throw new Error('Unable to load profiles parquet for SQL');
            }
        }

        // Execute the query
    const result = await duckdbLoader.query(query);
    const rows = result.toArray().map(r => Object.fromEntries(r));
    renderQueryResults(rows);
    showToast(`Query returned ${rows.length} row(s) (deduplicated in view)`, 'success');
        
    } catch (error) {
        console.error('Query failed:', error);
        showToast('Query failed: ' + error.message, 'error');
    } finally {
        showLoading(false);
    }
}

async function saveQuery() {
    showToast('Query save functionality coming soon', 'info');
}

// Initialize the app when DOM is loaded
document.addEventListener('DOMContentLoaded', init);

export { init, showToast, showLoading };

// Reload datasets: clears server caches and reloads DuckDB table
async function reloadDatasets() {
    try {
        showLoading(true);
        // Clear server-side caches if implemented
        try { await fetch('/api/data/reload', { method: 'POST' }); } catch {}
        // Reload selected dataset into DuckDB
        await duckdbLoader.init();
        const select = document.getElementById('dataset-select');
        const path = (select && select.value) || '/dataset/pdb_profiles.parquet';
        const meta = await duckdbLoader.loadParquetFile(path, 'profiles');
        if (!meta?.success) throw new Error(meta?.error || 'Failed to reload dataset');
    const status = document.getElementById('dataset-status');
        if (status) status.textContent = `Dataset: ${meta.filePath} (${meta.rowCount} rows)`;
        showToast('Datasets reloaded', 'success');
    try { await refreshDatasetSelect(); } catch {}
    } catch (e) {
        console.error('Reload datasets failed:', e);
        showToast('Reload failed: ' + e.message, 'error');
    } finally {
        showLoading(false);
    }
}

// Export last query results as CSV or JSON
function exportQueryResults(fmt) {
    try {
        if (!lastQueryRows || lastQueryRows.length === 0) {
            showToast('No results to export', 'error');
            return;
        }
    if (fmt === 'json') {
            const blob = new Blob([JSON.stringify(lastQueryRows, null, 2)], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            download(url, 'query_results.json');
            URL.revokeObjectURL(url);
            showToast('Exported JSON', 'success');
        } else if (fmt === 'csv') {
            const cols = Object.keys(lastQueryRows[0]);
            const lines = [];
            lines.push(cols.join(','));
            for (const row of lastQueryRows) {
                const vals = cols.map(c => csvEscape(row[c]));
                lines.push(vals.join(','));
            }
            const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
            const url = URL.createObjectURL(blob);
            download(url, 'query_results.csv');
            URL.revokeObjectURL(url);
            showToast('Exported CSV', 'success');
        } else if (fmt === 'parquet') {
            const filename = prompt('Enter parquet filename', `query_export_${Date.now()}.parquet`) || `query_export_${Date.now()}.parquet`;
            fetch('/api/data/export-parquet', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ rows: lastQueryRows, filename })
            }).then(async (resp) => {
                const json = await resp.json();
                if (!resp.ok || !json.success) throw new Error(json.error || 'Export failed');
                showToast(`Exported Parquet: ${json.filename}`, 'success');
                // Offer to load the exported file into Query Builder
                const loadNow = confirm('Load exported Parquet into Query Builder now?');
                if (loadNow) {
                    try {
                        await duckdbLoader.init();
                        const rel = (json && (json.relative || ('exports/' + json.filename))) || ('exports/' + json.filename);
                        const exportedUrl = '/dataset/' + rel;
                        // Ensure it appears in selector under Exports
                        const select = document.getElementById('dataset-select');
                        if (select) {
                            let group = Array.from(select.children).find(n => n.tagName === 'OPTGROUP' && n.label === 'Exports');
                            if (!group) { group = document.createElement('optgroup'); group.label = 'Exports'; select.appendChild(group); }
                            // Add option if missing
                            const exists = Array.from(group.children).some(o => o.value === exportedUrl);
                            if (!exists) { const o = document.createElement('option'); o.value = exportedUrl; o.textContent = rel; group.appendChild(o); }
                        }
                        const meta = await duckdbLoader.loadParquetFile(exportedUrl, 'profiles');
                        if (meta?.success) {
                            if (select) { select.value = exportedUrl; localStorage.setItem('viewer.dataset.path', exportedUrl); }
                            const status = document.getElementById('dataset-status');
                            if (status) status.textContent = `Dataset: ${exportedUrl} (${meta.rowCount} rows)`;
                            showToast(`Loaded exported dataset (${meta.rowCount} rows)`, 'success');
                        } else {
                            showToast('Exported file saved, but failed to load in DuckDB', 'error');
                        }
                    } catch (e) {
                        console.error('Auto-load exported parquet failed:', e);
                        showToast('Failed to load exported parquet: ' + e.message, 'error');
                    }
                }
                // Refresh selector listing to include the new export
                try { await refreshDatasetSelect(); } catch {}
            }).catch(e => {
                console.error('Parquet export failed:', e);
                showToast('Parquet export failed: ' + e.message, 'error');
            });
        }
    } catch (e) {
        console.error('Export failed:', e);
        showToast('Export failed: ' + e.message, 'error');
    }
}

function csvEscape(val) {
    if (val === null || val === undefined) return '';
    const s = String(val);
    if (s.includes('"') || s.includes(',') || s.includes('\n')) {
        return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
}

function download(url, filename) {
    const a = document.createElement('a');
    a.href = url; a.download = filename; a.style.display = 'none';
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
}

// Trigger backend cleaning, refresh selector, and let user load a cleaned file
async function cleanDatasets() {
    try {
        showLoading(true);
        const btn = document.getElementById('clean-datasets-btn');
        const status = document.getElementById('clean-status');
        if (btn) btn.disabled = true;
        if (status) status.textContent = 'Cleaning…';
        const resp = await fetch('/api/data/clean-datasets', { method: 'POST' });
        const json = await resp.json();
        if (!resp.ok || !json.success) throw new Error(json.error || 'Cleaning failed');
        showToast('Datasets cleaned; outputs saved under Exports', 'success');
        await refreshDatasetSelect();
        try {
            const parts = (json.results || []).map(r => {
                const name = (r.out || '').split('/').pop();
                return `${name || 'out'}: ${r.rows || 0}`;
            });
            if (status) status.textContent = parts.length ? `Cleaned (${parts.join(' | ')})` : 'Cleaned';
        } catch {}
        // Attempt to pick the cleaned merged file if present
        const results = Array.isArray(json.results) ? json.results : [];
        const merged = results.find(r => r.out && r.out.includes('pdb_profiles_merged_cleaned_')) || results[0];
        if (merged && merged.out) {
            const rel = merged.out.split('/data/bot_store/')[1] || merged.out.split('/dataset/')[1] || '';
            const path = rel ? '/dataset/' + rel.replace(/^.*bot_store\//, '') : '/dataset/exports/' + (merged.out.split('/').pop() || '');
            const loadNow = confirm('Load cleaned dataset now?');
            if (loadNow) {
                await switchDataset(path);
            }
        }
    } catch (e) {
        console.error('Clean datasets failed:', e);
        showToast('Clean failed: ' + e.message, 'error');
        const status = document.getElementById('clean-status');
        if (status) status.textContent = 'Clean failed';
    } finally {
        showLoading(false);
        const btn = document.getElementById('clean-datasets-btn');
        if (btn) btn.disabled = false;
    }
}

// Backfill missing fields by invoking backend orchestrator (v1/v2 scrape + re-export)
async function fillMissingData() {
    try {
        showLoading(true);
        const btn = document.getElementById('fill-missing-btn');
        const status = document.getElementById('fill-status');
        if (btn) btn.disabled = true;
        if (status) status.textContent = 'Backfilling v1…';
        const resp = await fetch('/api/data/fill-missing', { method: 'POST' });
        const json = await resp.json();
        if (!resp.ok || !json.success) throw new Error(json.error || 'Backfill failed');
        showToast(`Backfill complete: ${json.summary || ''}`, 'success');
        if (status) status.textContent = 'Exporting normalized…';
        // Refresh selector and dataset status
        try { await refreshDatasetSelect(); } catch {}
        try { await reloadDatasets(); } catch {}
        // Refresh profiles + vectors for KNN alignment
        try {
            const resp = await fetch('/api/data/profiles');
            const data = await resp.json();
            if (!resp.ok || data.error) throw new Error(data.error || `HTTP ${resp.status}`);
            currentData = data.profiles || [];
            knnSearch.setProfiles(currentData);
            await knnSearch.loadVectors();
            filteredData = dedupeByCid(currentData);
            renderResults();
        } catch {}
        if (status) status.textContent = 'Done';
        // Offer to switch to normalized parquet for queries
        try {
            const ok = confirm('Switch Query Builder to normalized dataset now?');
            if (ok) {
                await switchDataset('/dataset/pdb_profiles_normalized.parquet');
                const select = document.getElementById('dataset-select');
                if (select) { select.value = '/dataset/pdb_profiles_normalized.parquet'; localStorage.setItem('viewer.dataset.path', '/dataset/pdb_profiles_normalized.parquet'); }
            }
        } catch {}
    } catch (e) {
        console.error('Fill missing failed:', e);
        showToast('Fill missing failed: ' + e.message, 'error');
        const status = document.getElementById('fill-status');
        if (status) status.textContent = 'Failed';
    } finally {
        showLoading(false);
        const btn = document.getElementById('fill-missing-btn');
        if (btn) btn.disabled = false;
    }
}

// Refresh dataset selector options from /dataset while preserving selection when possible
async function refreshDatasetSelect() {
    const select = document.getElementById('dataset-select');
    if (!select) return;
    const current = select.value;
    try {
        const resp = await fetch('/dataset');
        const info = await resp.json();
        const baseOptions = [
            { value: '/dataset/pdb_profiles.parquet', label: 'Main (pdb_profiles.parquet)' },
            { value: '/dataset/pdb_profiles_normalized.parquet', label: 'Normalized (pdb_profiles_normalized.parquet)' },
            { value: '/dataset/pdb_profiles_merged.parquet', label: 'Merged (pdb_profiles_merged.parquet)' }
        ];
        select.innerHTML = '';
        for (const opt of baseOptions) {
            const o = document.createElement('option'); o.value = opt.value; o.textContent = opt.label; select.appendChild(o);
        }
        const exportsList = Array.isArray(info.exports) ? info.exports : [];
        if (exportsList.length) {
            const group = document.createElement('optgroup'); group.label = 'Exports';
            for (const rel of exportsList) {
                const full = '/dataset/' + rel;
                const o = document.createElement('option'); o.value = full; o.textContent = rel; group.appendChild(o);
            }
            select.appendChild(group);
        }
        // Restore selection if still available
        const allOptions = Array.from(select.querySelectorAll('option'));
        if (allOptions.some(o => o.value === current)) {
            select.value = current;
        }
    } catch (e) {
        console.warn('Failed to refresh dataset selector:', e);
    }
}