// Add Personality UI and logic
window.addEventListener('DOMContentLoaded', () => {
  const form = document.createElement('form');
  form.id = 'add-personality-form';
  form.innerHTML = `
    <h2>Add Personality</h2>
    <label>Name <input name="label" required></label>
    <label>X <input name="x" type="number" min="-1" max="1" step="0.01" required></label>
    <label>Y <input name="y" type="number" min="-1" max="1" step="0.01" required></label>
    <label>Z <input name="z" type="number" min="-1" max="1" step="0.01" required></label>
    <label>Quadrant Color
      <select name="color">
        <option value="#e6194b">Quadrant 1 (Red)</option>
        <option value="#3cb44b">Quadrant 2 (Green)</option>
        <option value="#ffe119">Quadrant 3 (Yellow)</option>
        <option value="#4363d8">Quadrant 4 (Blue)</option>
        <option value="#000000">You (Black)</option>
      </select>
    </label>
    <label>Type <input name="type" placeholder="e.g. celebrity, fictional, user"></label>
    <label>Socionics <input name="socionics" placeholder="e.g. ILE, SEI, etc."></label>
    <label>Description <textarea name="description" rows="2" placeholder="Short description..."></textarea></label>
    <button type="submit">Add</button>
  `;
  document.body.appendChild(form);

  form.addEventListener('submit', function(e) {
    e.preventDefault();
    const fd = new FormData(form);
    const p = {
      label: fd.get('label'),
      x: parseFloat(fd.get('x')),
      y: parseFloat(fd.get('y')),
      z: parseFloat(fd.get('z')),
      color: fd.get('color'),
      size: 0.09,
      type: fd.get('type') || undefined,
      socionics: fd.get('socionics') || undefined,
      description: fd.get('description') || undefined
    };
    if (!window.personalityData) window.personalityData = [];
    window.personalityData.push(p);
    if (window.refreshCompass) window.refreshCompass();
    form.reset();
  });
});
