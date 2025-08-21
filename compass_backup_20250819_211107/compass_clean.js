import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { CSS2DRenderer, CSS2DObject } from 'three/examples/jsm/renderers/CSS2DRenderer.js';

window.addEventListener('DOMContentLoaded', () => {
  console.log('DOMContentLoaded: compass_clean.js loaded');

  const container = document.getElementById('compass-container');
  if (!container) {
    console.error('No #compass-container found!');
    debugDiv.textContent = 'No #compass-container found!';
    return;
  }
  const width = container.offsetWidth || window.innerWidth;
  const height = container.offsetHeight || window.innerHeight * 0.9;

  // Scene, camera, renderer
  const scene = new THREE.Scene();
  scene.background = new THREE.Color(0xf8f8f8);
  const camera = new THREE.PerspectiveCamera(60, width / height, 0.1, 1000);
  camera.position.set(3, 3, 5);
  camera.lookAt(0, 0, 0);

  const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
  renderer.setSize(width, height);
  container.appendChild(renderer.domElement);
  console.log('Renderer appended to #compass-container');

  // Add soft light
  const light = new THREE.AmbientLight(0xffffff, 0.7);
  scene.add(light);
  const dirLight = new THREE.DirectionalLight(0xffffff, 0.5);
  dirLight.position.set(5, 5, 5);
  scene.add(dirLight);

  // Axes helper
  scene.add(new THREE.AxesHelper(2));

  // Draw cube (wireframe, thicker lines)
  const geometry = new THREE.BoxGeometry(2, 2, 2);
  const edges = new THREE.EdgesGeometry(geometry);
  const line = new THREE.LineSegments(edges, new THREE.LineBasicMaterial({ color: 0x333333, linewidth: 2 }));
  scene.add(line);

  // Draw octant planes (subtle)
  const planeMaterial = new THREE.LineBasicMaterial({ color: 0xcccccc, opacity: 0.5, transparent: true });
  const drawPlane = (axis, pos) => {
    const points = [];
    if (axis === 'x') {
      points.push(new THREE.Vector3(pos, -1, -1), new THREE.Vector3(pos, 1, -1), new THREE.Vector3(pos, 1, 1), new THREE.Vector3(pos, -1, 1), new THREE.Vector3(pos, -1, -1));
    } else if (axis === 'y') {
      points.push(new THREE.Vector3(-1, pos, -1), new THREE.Vector3(1, pos, -1), new THREE.Vector3(1, pos, 1), new THREE.Vector3(-1, pos, 1), new THREE.Vector3(-1, pos, -1));
    } else if (axis === 'z') {
      points.push(new THREE.Vector3(-1, -1, pos), new THREE.Vector3(1, -1, pos), new THREE.Vector3(1, 1, pos), new THREE.Vector3(-1, 1, pos), new THREE.Vector3(-1, -1, pos));
    }
    const planeGeo = new THREE.BufferGeometry().setFromPoints(points);
    const plane = new THREE.Line(planeGeo, planeMaterial);
    scene.add(plane);
  };
  drawPlane('x', 0);
  drawPlane('y', 0);
  drawPlane('z', 0);



  // Add axis labels to the scene
  addAxisLabels(scene);

  // Center controls on the cube
  const controls = new OrbitControls(camera, renderer.domElement);
  controls.target.set(0, 0, 0);
  controls.update();


  // CSS2DRenderer for labels and tooltips
  const labelRenderer = new CSS2DRenderer();
  labelRenderer.setSize(width, height);
  labelRenderer.domElement.style.position = 'absolute';
  labelRenderer.domElement.style.top = '0';
  labelRenderer.domElement.style.pointerEvents = 'none';
  container.appendChild(labelRenderer.domElement);

  // Plot personalities with icons, halos, and tooltips
  function plotPersonalities() {
    // Remove old points/labels
    for (let obj of scene.children.slice()) {
      if (obj.userData && obj.userData.isPersonality) scene.remove(obj);
    }
    if (!window.personalityData) return;
    // If any are _selected, only plot those; else plot all
    let toPlot = window.personalityData.some(p => p._selected) ? window.personalityData.filter(p => p._selected) : window.personalityData;
    // Apply color filter if present
    if (window.personalityData.some(p => p._colorFiltered)) {
      toPlot = toPlot.filter(p => !p._colorFiltered);
    }
  toPlot.forEach(person => {
      // Sphere marker
      const geometry = new THREE.SphereGeometry(person.size || 0.09, 32, 32);
      const material = new THREE.MeshBasicMaterial({ color: person.color });
      const sphere = new THREE.Mesh(geometry, material);
      sphere.position.set(person.x, person.y, person.z);
      sphere.userData = { isPersonality: true };
      scene.add(sphere);

      // Halo (2D circle overlay)
      const haloDiv = document.createElement('div');
      haloDiv.style.width = '38px';
      haloDiv.style.height = '38px';
      haloDiv.style.borderRadius = '50%';
      haloDiv.style.border = `3px solid ${person.color}`;
      haloDiv.style.background = 'rgba(255,255,255,0.0)';
      haloDiv.style.position = 'absolute';
      haloDiv.style.boxSizing = 'border-box';
      haloDiv.style.pointerEvents = 'auto';
      haloDiv.style.zIndex = 2;
      // Icon (optional, fallback to label initial)
      const icon = document.createElement('div');
      icon.style.width = '32px';
      icon.style.height = '32px';
      icon.style.borderRadius = '50%';
      icon.style.background = '#fff';
      icon.style.display = 'flex';
      icon.style.alignItems = 'center';
      icon.style.justifyContent = 'center';
      icon.style.fontWeight = 'bold';
      icon.style.fontSize = '1.1em';
      icon.style.color = person.color;
      icon.textContent = person.label ? person.label[0] : '?';
      haloDiv.appendChild(icon);

      // Tooltip with hyperlink
      const tooltip = document.createElement('div');
      tooltip.className = 'point-label';
      tooltip.style.display = 'none';
      tooltip.innerHTML = `<a href="https://www.personality-database.com/profile?name=${encodeURIComponent(person.label)}" target="_blank" style="color:${person.color};text-decoration:underline;">${person.label}</a>`;
      haloDiv.appendChild(tooltip);

      // Hover logic
      haloDiv.onmouseenter = () => { tooltip.style.display = 'block'; };
      haloDiv.onmouseleave = () => { tooltip.style.display = 'none'; };

      const labelObj = new CSS2DObject(haloDiv);
      labelObj.position.set(person.x, person.y, person.z + (person.size || 0.09) + 0.05);
      scene.add(labelObj);
    });
  }

  // Expose for search integration
  window.refreshCompass = plotPersonalities;
  plotPersonalities();

  // Render loop
  function animate() {
    requestAnimationFrame(animate);
    controls.update();
    renderer.render(scene, camera);
    labelRenderer.render(scene, camera);
  }
  animate();

  // Add axis and quadrant labels for clarity
  function addAxisLabels(scene) {
    const axisLabels = [
      { pos: [1.2, 0, 0], text: 'X+', color: '#e6194b', desc: 'E/I' },
      { pos: [-1.2, 0, 0], text: 'X-', color: '#3cb44b', desc: 'L/S' },
      { pos: [0, 1.2, 0], text: 'Y+', color: '#ffe119', desc: 'F/T' },
      { pos: [0, -1.2, 0], text: 'Y-', color: '#4363d8', desc: 'R/P' },
      { pos: [0, 0, 1.2], text: 'Z+', color: '#f58231', desc: 'Intuition' },
      { pos: [0, 0, -1.2], text: 'Z-', color: '#911eb4', desc: 'Sensing' },
    ];
    axisLabels.forEach(lab => {
      const div = document.createElement('div');
      div.className = 'axis-label';
      div.innerHTML = `<b>${lab.text}</b><br><span style="font-size:0.9em">${lab.desc}</span>`;
      div.style.color = lab.color;
      div.style.background = 'rgba(255,255,255,0.85)';
      div.style.borderRadius = '6px';
      div.style.padding = '2px 8px';
  div.style.fontWeight = 'bold';
  div.style.textAlign = 'center';
  div.style.boxShadow = '0 1px 4px rgba(0,0,0,0.07)';
  div.style.fontSize = '1.6em';
      const label = new CSS2DObject(div);
      label.position.set(...lab.pos);
      scene.add(label);
    });
  }


  // Add axis legend overlay
  function addAxisLegend() {
    let legend = document.getElementById('axis-legend');
    if (legend) legend.remove();
    legend = document.createElement('div');
    legend.id = 'axis-legend';
    legend.style.position = 'absolute';
    legend.style.left = '50%';
    legend.style.top = '4.5em';
    legend.style.transform = 'translateX(-50%)';
    legend.style.background = 'rgba(255,255,255,0.97)';
    legend.style.borderRadius = '10px';
    legend.style.boxShadow = '0 2px 12px rgba(0,0,0,0.08)';
    legend.style.padding = '1em 2em';
    legend.style.zIndex = 1002;
    legend.style.fontSize = '1.1em';
    legend.style.textAlign = 'center';
    legend.innerHTML = `
      <b>Axis Legend</b><br>
      <span style="color:#e6194b">X</span>: E/I &nbsp; | &nbsp;
      <span style="color:#ffe119">Y</span>: F/T &nbsp; | &nbsp;
      <span style="color:#f58231">Z</span>: Intuition/Sensing<br>
      <span style="font-size:0.98em;color:#888;">Color wheel: 2D type/quadra mapping</span>
    `;
    document.body.appendChild(legend);
  }
  addAxisLegend();

});
