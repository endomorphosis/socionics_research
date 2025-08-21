import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { CSS2DRenderer, CSS2DObject } from 'three/examples/jsm/renderers/CSS2DRenderer.js';

window.addEventListener('DOMContentLoaded', () => {
  const container = document.getElementById('compass-container');
  const width = container.offsetWidth || window.innerWidth;
  const height = container.offsetHeight || window.innerHeight * 0.9;

  // Scene, camera, renderer
  const scene = new THREE.Scene();
  scene.background = new THREE.Color(0xf8f8f8);
  const camera = new THREE.PerspectiveCamera(60, width / height, 0.1, 1000);
  camera.position.set(3, 3, 5);

  const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
  renderer.setSize(width, height);
  container.appendChild(renderer.domElement);

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

    // Add axis and quadrant labels for clarity
    // --- AXIS LABELS ---
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
        const label = new CSS2DObject(div);
        label.position.set(...lab.pos);
        scene.add(label);
      });
    }
  
    // --- QUADRANT LABELS (2D overlay) ---
    function addQuadrantLegend() {
      if (document.getElementById('quadrant-legend')) return;
      const legend = document.createElement('div');
      legend.id = 'quadrant-legend';
      legend.innerHTML = `
        <h2>3D Quadrant Key</h2>
        <div style="display:flex;align-items:center;justify-content:center;margin-bottom:0.7em;">
          <svg width="110" height="110" viewBox="0 0 110 110">
            <g>
              <path d="M55,55 L105,55 A50,50 0 0,1 55,5 Z" fill="#e6194b"/>
              <path d="M55,55 L55,5 A50,50 0 0,1 5,55 Z" fill="#3cb44b"/>
              <path d="M55,55 L5,55 A50,50 0 0,1 55,105 Z" fill="#ffe119"/>
              <path d="M55,55 L55,105 A50,50 0 0,1 105,55 Z" fill="#4363d8"/>
            </g>
            <circle cx="55" cy="55" r="50" fill="none" stroke="#bbb" stroke-width="2"/>
            <text x="100" y="53" font-size="12" fill="#e6194b" text-anchor="start" alignment-baseline="middle">+X</text>
            <text x="10" y="53" font-size="12" fill="#3cb44b" text-anchor="end" alignment-baseline="middle">-X</text>
            <text x="55" y="12" font-size="12" fill="#ffe119" text-anchor="middle" alignment-baseline="middle">+Y</text>
            <text x="55" y="105" font-size="12" fill="#4363d8" text-anchor="middle" alignment-baseline="middle">-Y</text>
          </svg>
        </div>
        <div style="font-size:0.98em;margin-bottom:0.7em;">
          <b>X axis:</b> Extraversion (+X, red) / Introversion (−X, green)<br>
          <b>Y axis:</b> Logic (+Y, yellow) / Sensing (−Y, blue)<br>
          <b>Z axis:</b> Intuition (+Z, orange) / Sensing (−Z, purple)
        </div>
        <div class="legend-row"><span class="legend-color" style="background:#e6194b"></span> (+X, +Y, +Z) — Quadrant 1</div>
        <div class="legend-row"><span class="legend-color" style="background:#3cb44b"></span> (−X, +Y, +Z) — Quadrant 2</div>
        <div class="legend-row"><span class="legend-color" style="background:#ffe119"></span> (+X, −Y, +Z) — Quadrant 3</div>
        <div class="legend-row"><span class="legend-color" style="background:#4363d8"></span> (−X, −Y, +Z) — Quadrant 4</div>
        <div class="legend-row"><span class="legend-color" style="background:#f58231"></span> (+X, +Y, −Z) — Quadrant 5</div>
        <div class="legend-row"><span class="legend-color" style="background:#911eb4"></span> (−X, +Y, −Z) — Quadrant 6</div>
        <div class="legend-row"><span class="legend-color" style="background:#46f0f0"></span> (+X, −Y, −Z) — Quadrant 7</div>
        <div class="legend-row"><span class="legend-color" style="background:#f032e6"></span> (−X, −Y, −Z) — Quadrant 8</div>
      `;
      legend.style.position = 'absolute';
      legend.style.bottom = '2em';
      legend.style.right = '2em';
      legend.style.background = 'rgba(255,255,255,0.97)';
      legend.style.borderRadius = '10px';
      legend.style.boxShadow = '0 2px 12px rgba(0,0,0,0.08)';
      legend.style.padding = '1em 1.5em';
      legend.style.zIndex = 20;
      legend.style.minWidth = '260px';
      legend.style.fontSize = '1em';
      document.body.appendChild(legend);
    }

    addAxisLabels(scene);
    addQuadrantLegend();


  // OrbitControls for interactivity
  let controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true;
  controls.dampingFactor = 0.1;
  controls.minDistance = 2;
  controls.maxDistance = 10;
  controls.target.set(0, 0, 0);
  controls.update();

  // CSS2DRenderer for labels
  let labelRenderer = new CSS2DRenderer();
  labelRenderer.setSize(width, height);
  labelRenderer.domElement.style.position = 'absolute';
  labelRenderer.domElement.style.top = '0';
  labelRenderer.domElement.style.pointerEvents = 'none';
  container.appendChild(labelRenderer.domElement);

  // Store spheres and labels for refresh
  let spheres = [];

  function plotPoints() {
    // Remove old spheres
    spheres.forEach(obj => scene.remove(obj));
    spheres = [];
    if (!window.personalityData) return;
    // Filtering
    let filters = { colors: null, types: null };
    if (window.getCompassFilters) filters = window.getCompassFilters();
    window.personalityData.forEach(p => {
      // Type logic: use explicit type if present, otherwise fallback to label-based logic
      let type = p.type || 'user';
      if (!p.type) {
        if (p.label && /^Celebrity/i.test(p.label)) type = 'celebrity';
        else if (p.label && /fictional/i.test(p.label)) type = 'fictional';
        else if (p.label && p.label === 'You') type = 'user';
      }
      p._type = type;
      // Filter by color and type
      if (filters.colors && !filters.colors.includes(p.color)) return;
      if (filters.types && !filters.types.includes(type)) return;
      // Highlight logic
      let highlight = false;
      if (window.highlightedPersonality && p.label && p.label.toLowerCase().includes(window.highlightedPersonality)) highlight = true;
      const color = new THREE.Color(p.color);
      const geometry = new THREE.SphereGeometry((highlight ? 0.14 : (p.size || 0.06)), 24, 24);
      const material = new THREE.MeshPhongMaterial({ color, shininess: 60 });
      const sphere = new THREE.Mesh(geometry, material);
      sphere.position.set(p.x, p.y, p.z);
      if (highlight) {
        // Add outline
        const outlineGeo = new THREE.SphereGeometry((highlight ? 0.16 : (p.size || 0.08)), 24, 24);
        const outlineMat = new THREE.MeshBasicMaterial({ color: 0x000000, side: THREE.BackSide });
        const outline = new THREE.Mesh(outlineGeo, outlineMat);
        sphere.add(outline);
      }
      // Attach personality data for tooltip
      sphere.userData.personality = p;
      scene.add(sphere);
      spheres.push(sphere);
      if (p.label && labelRenderer) {
        const labelDiv = document.createElement('div');
        labelDiv.className = 'point-label';
        labelDiv.textContent = p.label;
        labelDiv.style.color = p.color;
        labelDiv.style.fontWeight = (p.label === 'You' || highlight) ? 'bold' : 'normal';
        labelDiv.style.textShadow = '0 0 4px #fff, 0 0 2px #fff';
        if (highlight) labelDiv.style.background = '#ffe119';
        const label = new CSS2DObject(labelDiv);
        label.position.set(0, (highlight ? 0.16 : (p.size || 0.06)) + 0.05, 0);
        sphere.add(label);
      }
    });
    // Setup tooltip if available
    if (window.setupCompassTooltip) window.setupCompassTooltip(renderer, camera, scene, spheres);
  }

  // Expose refresh function for UI
  window.refreshCompass = plotPoints;

  // Responsive resize
  window.addEventListener('resize', () => {
    const w = container.offsetWidth || window.innerWidth;
    const h = container.offsetHeight || window.innerHeight * 0.9;
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
    renderer.setSize(w, h);
    if (labelRenderer) labelRenderer.setSize(w, h);
  });

  // Animation loop
  function animate() {
    requestAnimationFrame(animate);
    if (controls) controls.update();
    renderer.render(scene, camera);
    if (labelRenderer) labelRenderer.render(scene, camera);
  }
  animate();
});

