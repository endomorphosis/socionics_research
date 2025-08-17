// Search UI and logic for highlighting personalities

window.addEventListener('DOMContentLoaded', () => {
  // RAG Search Panel (fixed, always visible, styled)
  const searchDiv = document.createElement('div');
  searchDiv.id = 'search-controls';
  searchDiv.style.position = 'fixed';
  searchDiv.style.top = '2em';
  searchDiv.style.left = '2em';
  searchDiv.style.width = '340px';
  searchDiv.style.maxWidth = '90vw';
  searchDiv.style.background = 'rgba(255,255,255,0.98)';
  searchDiv.style.borderRadius = '12px';
  searchDiv.style.boxShadow = '0 2px 16px rgba(0,0,0,0.13)';
  searchDiv.style.padding = '1.2em 1.2em 1em 1.2em';
  searchDiv.style.zIndex = 1005;
  searchDiv.style.display = 'flex';
  searchDiv.style.flexDirection = 'column';
  searchDiv.style.alignItems = 'stretch';
  searchDiv.innerHTML = `
    <label for="search-input" style="font-weight:bold;font-size:1.1em;margin-bottom:0.5em;">RAG Search (Parquet DB)</label>
    <input id="search-input" type="text" placeholder="Search for a celebrity..." autocomplete="off" style="padding:0.7em 1em;font-size:1.1em;border-radius:8px;border:1.5px solid #bbb;margin-bottom:0.7em;" />
    <div id="search-results" style="max-height:260px;overflow-y:auto;"></div>
  `;
  document.body.appendChild(searchDiv);

  const input = searchDiv.querySelector('#search-input');
  const resultsDiv = searchDiv.querySelector('#search-results');

  async function renderResults(query) {
    const q = query.trim();
    if (!q) {
      resultsDiv.innerHTML = '';
      return;
    }
    resultsDiv.innerHTML = '<div style="color:#888;padding:0.5em;">Searching...</div>';
    try {
  const resp = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
      const filtered = await resp.json();
      resultsDiv.innerHTML = '';
      if (!filtered.length) {
        resultsDiv.innerHTML = '<div style="color:#888;padding:0.5em;">No results found.</div>';
        return;
      }
      filtered.forEach(person => {
        const row = document.createElement('div');
        row.style.display = 'flex';
        row.style.alignItems = 'center';
        row.style.gap = '0.7em';
        row.style.padding = '0.4em 0.7em';
        row.style.cursor = 'pointer';
        row.style.borderRadius = '6px';
        row.style.transition = 'background 0.15s';
        row.onmouseenter = () => row.style.background = '#f5f5f5';
        row.onmouseleave = () => row.style.background = 'none';
        row.onclick = () => {
          if (!window.personalityData) window.personalityData = [];
          window.personalityData.push(person);
          if (window.refreshCompass) window.refreshCompass();
          renderResults(input.value); // update highlight
        };
        // Icon
        const icon = document.createElement('div');
        icon.style.width = '28px';
        icon.style.height = '28px';
        icon.style.borderRadius = '50%';
        icon.style.background = '#fff';
        icon.style.display = 'flex';
        icon.style.alignItems = 'center';
        icon.style.justifyContent = 'center';
        icon.style.fontWeight = 'bold';
        icon.style.fontSize = '1em';
        icon.style.color = person.color;
        icon.textContent = person.label ? person.label[0] : '?';
        row.appendChild(icon);
        // Name
        const name = document.createElement('span');
        name.textContent = person.label;
        name.style.fontWeight = '500';
        name.style.color = person.color;
        row.appendChild(name);
        resultsDiv.appendChild(row);
      });
    } catch (err) {
      resultsDiv.innerHTML = `<div style='color:#c00;padding:0.5em;'>Error: ${err.message}</div>`;
    }
  }

  input.addEventListener('input', () => {
    renderResults(input.value);
  });

  // Initial render
  renderResults('');
});
