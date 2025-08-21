// Legend and quadrant color explanations
window.renderLegend = function() {
  const legend = document.createElement('div');
  legend.id = 'legend';
  legend.innerHTML = `
    <h2>Quadrant Colors</h2>
    <div class="legend-row"><span class="legend-color" style="background:#e6194b"></span> Quadrant 1</div>
    <div class="legend-row"><span class="legend-color" style="background:#3cb44b"></span> Quadrant 2</div>
    <div class="legend-row"><span class="legend-color" style="background:#ffe119"></span> Quadrant 3</div>
    <div class="legend-row"><span class="legend-color" style="background:#4363d8"></span> Quadrant 4</div>
    <div class="legend-row"><span class="legend-color" style="background:#000000"></span> You (Survey Participant)</div>
  `;
  document.body.appendChild(legend);
};
window.addEventListener('DOMContentLoaded', window.renderLegend);
