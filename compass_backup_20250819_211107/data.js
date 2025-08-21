import iro from '@jaames/iro';
// Add color wheel filter UI
window.addEventListener('DOMContentLoaded', () => {
  const colorDiv = document.createElement('div');
  colorDiv.id = 'color-filter-controls';
  colorDiv.style.position = 'fixed';
  colorDiv.style.bottom = '2em';
  colorDiv.style.right = '2em';
  colorDiv.style.background = 'rgba(255,255,255,0.97)';
  colorDiv.style.borderRadius = '10px';
  colorDiv.style.boxShadow = '0 2px 12px rgba(0,0,0,0.08)';
  colorDiv.style.padding = '1em 1.5em';
  colorDiv.style.zIndex = 1003;
  colorDiv.innerHTML = `
    <label style="font-weight:bold;">Filter by Color Proximity</label><br>
    <div id="iro-color-wheel" style="margin:0.5em auto 0.5em auto;"></div>
    <input type="range" id="color-tolerance" min="5" max="100" value="30" style="width:90px;vertical-align:middle;">
    <span id="color-tolerance-label">30</span>
  `;
  document.body.appendChild(colorDiv);

  const toleranceInput = colorDiv.querySelector('#color-tolerance');
  const toleranceLabel = colorDiv.querySelector('#color-tolerance-label');


  // Use iro.js color wheel
  const colorWheel = new iro.ColorPicker('#iro-color-wheel', {
    width: 140,
    color: '#e6194b',
    layout: [
      { component: iro.ui.Wheel }
    ]
  });

  function hexToRgb(hex) {
    const m = hex.match(/^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i);
    if (!m) return [0,0,0];
    return [parseInt(m[1],16), parseInt(m[2],16), parseInt(m[3],16)];
  }
  function colorDist(c1, c2) {
    // Euclidean distance in RGB
    return Math.sqrt((c1[0]-c2[0])**2 + (c1[1]-c2[1])**2 + (c1[2]-c2[2])**2);
  }

  function applyColorFilter() {
    const selected = hexToRgb(colorWheel.color.hexString);
    const tol = parseInt(toleranceInput.value, 10);
    if (!window.personalityData) return;
    window.personalityData.forEach(p => {
      const rgb = hexToRgb(p.color || '#888');
      p._colorFiltered = colorDist(selected, rgb) > tol;
    });
    if (window.refreshCompass) window.refreshCompass();
  }

  colorWheel.on('color:change', applyColorFilter);
  toleranceInput.addEventListener('input', () => {
    toleranceLabel.textContent = toleranceInput.value;
    applyColorFilter();
  });
});

