// Tooltip logic for 3D compass points
window.addEventListener('DOMContentLoaded', () => {
  const tooltip = document.createElement('div');
  tooltip.id = 'compass-tooltip';
  tooltip.style.display = 'none';
  document.body.appendChild(tooltip);

  // Raycaster for mouse hover
  let renderer, camera, scene, spheres = [];
  function setupTooltip(_renderer, _camera, _scene, _spheres) {
    renderer = _renderer;
    camera = _camera;
    scene = _scene;
    spheres = _spheres;
    renderer.domElement.addEventListener('mousemove', onMouseMove);
    renderer.domElement.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
  }

  function onMouseMove(event) {
    if (!renderer || !camera || !scene) return;
    const rect = renderer.domElement.getBoundingClientRect();
    const mouse = {
      x: ((event.clientX - rect.left) / rect.width) * 2 - 1,
      y: -((event.clientY - rect.top) / rect.height) * 2 + 1
    };
    const raycaster = new THREE.Raycaster();
    raycaster.setFromCamera(mouse, camera);
    const intersects = raycaster.intersectObjects(spheres, true);
    if (intersects.length > 0) {
      let obj = intersects[0].object;
      while (obj.parent && !obj.userData.personality) obj = obj.parent;
      const p = obj.userData.personality;
      if (p) {
        tooltip.innerHTML = `<strong>${p.label}</strong><br>` +
          (p.type ? `<b>Type:</b> ${p.type}<br>` : '') +
          (p.socionics ? `<b>Socionics:</b> ${p.socionics}<br>` : '') +
          (p.description ? `<em>${p.description}</em><br>` : '');
        tooltip.style.display = 'block';
        tooltip.style.left = (event.clientX + 12) + 'px';
        tooltip.style.top = (event.clientY + 12) + 'px';
      }
    } else {
      tooltip.style.display = 'none';
    }
  }

  window.setupCompassTooltip = setupTooltip;
});
