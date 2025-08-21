// Export 3D compass as PNG image
window.addEventListener('DOMContentLoaded', () => {
  const btn = document.createElement('button');
  btn.id = 'export-image-btn';
  btn.textContent = 'Export Image (PNG)';
  btn.type = 'button';
  btn.style.marginTop = '0.7em';
  btn.style.width = '100%';
  // Place below import/export controls
  const controls = document.getElementById('import-export-controls');
  if (controls) controls.appendChild(btn);
  else document.body.appendChild(btn);

  btn.addEventListener('click', () => {
    // Find the main WebGL canvas
    const canvas = document.querySelector('#compass-container canvas');
    if (!canvas) return alert('Could not find 3D view.');
    const url = canvas.toDataURL('image/png');
    const a = document.createElement('a');
    a.href = url;
    a.download = 'socionics_compass.png';
    document.body.appendChild(a);
    a.click();
    setTimeout(() => document.body.removeChild(a), 100);
  });
});
