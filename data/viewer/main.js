import './scraper-client.js';
import { knnSearch } from './knn-search.js';
import { duckdbLoader } from './duckdb-loader.js';

// Global state
let currentData = [];
let filteredData = [];
let currentPage = 1;
let itemsPerPage = 50;
let isCardView = false;

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
        
        filteredData = [...currentData];
        
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
            // Load profiles parquet into DuckDB for SQL queries (fallback to normalized handled later)
            const meta = await duckdbLoader.loadParquetFile('/dataset/pdb_profiles.parquet', 'profiles');
            if (!meta?.success) {
                // Try normalized file if main unavailable
                const meta2 = await duckdbLoader.loadParquetFile('/dataset/pdb_profiles_normalized.parquet', 'profiles');
                if (!meta2?.success) {
                    console.warn('DuckDB could not load profiles parquet:', meta?.error, meta2?.error);
                }
            }
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
    const sqlQuery = document.getElementById('sql-query');
    const templateBtns = document.querySelectorAll('.template-btn');

    executeQueryBtn.addEventListener('click', executeQuery);
    clearQueryBtn.addEventListener('click', () => sqlQuery.value = '');
    saveQueryBtn.addEventListener('click', saveQuery);
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
        
        filteredData = searchResults;
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
    filteredData = [...currentData];
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
    } catch (e) {
        console.error('Commit overlay failed:', e);
        showToast('Commit failed: ' + e.message, 'error');
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
    showToast('Deduplication coming soon', 'info');
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
    const url = document.getElementById('profile-url').value.trim();
    if (!url) {
        showToast('Please enter a profile URL', 'error');
        return;
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
            showToast('Profile scraped successfully!', 'success');
            // Add scraped profile to current data
            if (result.name) {
                const newProfile = {
                    cid: Date.now().toString(),
                    name: result.name || 'Unknown',
                    mbti: result.mbti || '',
                    socionics: result.socionics || '',
                    description: result.description || '',
                    category: 'Scraped Profile',
                    url: url
                };
                currentData.push(newProfile);
                filteredData = [...currentData];
                renderResults();
                showToast(`Added profile: ${result.name}`, 'success');
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
    if (!arrayRows || arrayRows.length === 0) {
        container.innerHTML = '<p class="no-results">No rows returned</p>';
        return;
    }
    const cols = Object.keys(arrayRows[0]);
    const table = document.createElement('table');
    table.className = 'results-table';
    const thead = document.createElement('thead');
    const trh = document.createElement('tr');
    cols.forEach(c => { const th = document.createElement('th'); th.textContent = c; trh.appendChild(th); });
    thead.appendChild(trh);
    const tbody = document.createElement('tbody');
    arrayRows.forEach(r => {
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
        showToast(`Query returned ${rows.length} row(s)`, 'success');
        
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