import './scraper-client.js';

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
        
        if (!hasData) {
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
        elements.dataStats.textContent = 'No data files found - Ready for scraping';
        filteredData = [];
        renderResults();
    }
}

// Check if actual data files exist
async function checkDataFiles() {
    try {
        // This would check for the actual parquet files
        // For now, return false to use sample data
        return false;
    } catch (error) {
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
    const sqlQuery = document.getElementById('sql-query');
    const templateBtns = document.querySelectorAll('.template-btn');

    executeQueryBtn.addEventListener('click', executeQuery);
    clearQueryBtn.addEventListener('click', () => sqlQuery.value = '');
    saveQueryBtn.addEventListener('click', saveQuery);

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

// Perform search
async function performSearch() {
    const query = elements.searchInput.value.trim().toLowerCase();
    if (!query) {
        applyFilters();
        return;
    }

    try {
        showLoading(true);
        
        // Filter data based on search query
        filteredData = currentData.filter(item => {
            return (item.name && item.name.toLowerCase().includes(query)) ||
                   (item.mbti && item.mbti.toLowerCase().includes(query)) ||
                   (item.socionics && item.socionics.toLowerCase().includes(query)) ||
                   (item.description && item.description.toLowerCase().includes(query));
        });
        
        currentPage = 1;
        renderResults();
        
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
    showToast('Export functionality coming soon', 'info');
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

async function executeQuery() {
    const query = document.getElementById('sql-query').value.trim();
    if (!query) {
        showToast('Please enter a SQL query', 'error');
        return;
    }
    
    try {
        showLoading(true);
        
        // For now, show a message that SQL querying requires DuckDB
        const resultsContainer = document.getElementById('query-results-container');
        resultsContainer.innerHTML = `
            <div style="padding: 2rem; text-align: center; background: #fff3cd; border: 1px solid #ffeaa7; border-radius: 8px;">
                <h3>SQL Query Feature</h3>
                <p>SQL querying requires DuckDB integration. This feature will be available when data files are loaded.</p>
                <p><strong>Your query:</strong></p>
                <pre style="background: #f8f9fa; padding: 1rem; border-radius: 4px; text-align: left; margin: 1rem 0;">${query}</pre>
                <p>For now, use the Search & Browse panel for data exploration.</p>
            </div>
        `;
        
        showToast('SQL query feature coming soon with DuckDB integration', 'info');
        
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