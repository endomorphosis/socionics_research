// Filtering/grouping UI and logic
window.addEventListener('DOMContentLoaded', () => {
  const filterDiv = document.createElement('div');
  filterDiv.id = 'filter-controls';
  filterDiv.innerHTML = `
    <h2>Filter Personalities</h2>
    <label><input type="checkbox" name="quadrant" value="#e6194b" checked> Quadrant 1 (Red)</label>
    <label><input type="checkbox" name="quadrant" value="#3cb44b" checked> Quadrant 2 (Green)</label>
    <label><input type="checkbox" name="quadrant" value="#ffe119" checked> Quadrant 3 (Yellow)</label>
    <label><input type="checkbox" name="quadrant" value="#4363d8" checked> Quadrant 4 (Blue)</label>
    <label><input type="checkbox" name="quadrant" value="#000000" checked> You (Black)</label>
    <label><input type="checkbox" name="type" value="celebrity" checked> Celebrities</label>
    <label><input type="checkbox" name="type" value="fictional" checked> Fictional Characters</label>
    <label><input type="checkbox" name="type" value="user" checked> User Entries</label>
  `;
  document.body.appendChild(filterDiv);

  // Filtering logic
  function getFilters() {
    const checkedColors = Array.from(filterDiv.querySelectorAll('input[name="quadrant"]:checked')).map(cb => cb.value);
    const checkedTypes = Array.from(filterDiv.querySelectorAll('input[name="type"]:checked')).map(cb => cb.value);
    return { colors: checkedColors, types: checkedTypes };
  }

  window.getCompassFilters = getFilters;
  if (window.refreshCompass) window.refreshCompass();

  filterDiv.addEventListener('change', () => {
    if (window.refreshCompass) window.refreshCompass();
  });
});
