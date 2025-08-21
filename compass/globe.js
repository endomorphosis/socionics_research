// Minimal globe renderer using Three.js for the Personality Planet
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';

const GLOBE = (() => {
  let scene, camera, renderer, sphere, container, controls;
  let groupPoints, groupCentroids, groupLinks, groupLabels, groupGrid, groupAxis;
  let pointMeshes = [], centroidMeshes = [];
  let myMarker = null;
  let raycaster, mouse, tooltip;
  let lastData = { centroids: [], points: [] };
  let opts = { links: false, labels: false, tooltip: false, grid: true };
  // Reinin (tetrakis hexahedron) face colors (24 faces)
  let reininColors = new Array(24).fill(0).map(() => new THREE.Color(0.5, 0.5, 0.5));
  // Optional names for Reinin faces (0..23), provided by UI
  let reininFaceNames = null;
  // Track current surface for tooltip behavior: 0=prismatic,1=mbti,2=reinin
  let currentSurfaceMode = 0;

  // Great-circle normals and contour/axis settings
  let circleNormals = [
    new THREE.Vector3(1.0, 0.2, 0.1).normalize(),   // E vs I
    new THREE.Vector3(0.1, 1.0, 0.2).normalize(),   // N vs S
    new THREE.Vector3(0.2, 0.1, 1.0).normalize(),   // F vs T
    new THREE.Vector3(-0.7, 0.6, 0.1).normalize()   // P vs J
  ];
  let contourEnabled = true;
  let contourWidth = 0.09;
  let contourIntensity = 0.55;
  let contourColors = [
    new THREE.Color(0xffc107), // E/I guide
    new THREE.Color(0xe6194b), // N/S guide
    new THREE.Color(0x00bcd4), // F/T guide
    new THREE.Color(0x8bc34a)  // P/J guide
  ];
  // Polar axis rotation
  let polarQuat = new THREE.Quaternion();

  function init(containerEl) {
    container = containerEl;
  // Prepare scene
  const cw = container.clientWidth || 800;
  const ch = container.clientHeight || 600;
  scene = new THREE.Scene();
  camera = new THREE.PerspectiveCamera(50, 1, 0.1, 1000); // we keep aspect=1 and letterbox
  camera.position.set(0, 0, 3.2);
  camera.lookAt(0, 0, 0);

  renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    // Make canvas fill container; we'll letterbox a centered square via viewport/scissor
    renderer.setSize(cw, ch);
    renderer.setScissorTest(true);
    renderer.domElement.style.display = 'block';
    renderer.domElement.style.width = '100%';
    renderer.domElement.style.height = '100%';
    renderer.domElement.style.margin = '0 auto';
    container.appendChild(renderer.domElement);

    function updateViewport() {
      const w = container.clientWidth || cw; const h = container.clientHeight || ch;
      // Resize drawing buffer to container size
      renderer.setSize(w, h, false);
      // Compute centered square viewport
      const s = Math.min(w, h);
      const x = Math.floor((w - s) / 2);
      const y = Math.floor((h - s) / 2);
      renderer.setViewport(x, y, s, s);
      renderer.setScissor(x, y, s, s);
      // Keep camera aspect square
      camera.aspect = 1;
      camera.updateProjectionMatrix();
    }

    // Orbit-style controls for a Google Earth feel
    controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.08;
    controls.rotateSpeed = 0.8;
    controls.zoomSpeed = 0.6;
    controls.enablePan = false;

  const amb = new THREE.AmbientLight(0xffffff, 0.6);
    scene.add(amb);
  const dir = new THREE.DirectionalLight(0xffffff, 0.8);
  dir.position.set(4, 2, 2);
    scene.add(dir);

  const geo = new THREE.SphereGeometry(1, 128, 128);
    // Custom shader: encode 4 dichotomies as color quadrants, smoothly blended
    const dichotomyShader = {
      uniforms: {
        time: { value: 0.0 },
  colorMode: { value: 0 }, // 0=prismatic, 1=MBTI regions, 2=Reinin (tetrakis)
        MBTI_COUNT: { value: 16 },
        mbtiDirs: { value: new Array(16).fill(0).map(() => new THREE.Vector3(1,0,0)) },
        mbtiCols: { value: new Array(16).fill(0).map(() => new THREE.Vector3(1,1,1)) },
  reininCols: { value: new Array(24).fill(0).map(() => new THREE.Vector3(0.5,0.5,0.5)) },
  highlightIdx: { value: -1 },
        gcNormals: { value: new Array(4).fill(0).map(() => new THREE.Vector3(1,0,0)) },
        rotMat: { value: new THREE.Matrix3().identity() },
        showContours: { value: 1 },
        contourWidth: { value: contourWidth },
  contourCols: { value: new Array(4).fill(0).map(() => new THREE.Vector3(1,1,1)) },
  contourIntensity: { value: contourIntensity }
      },
      vertexShader: `
        varying vec2 vUv;
        varying vec3 vPos;
        void main() {
          vUv = uv;
          vPos = normalize(position);
          gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
        }
      `,
      fragmentShader: `
        varying vec2 vUv;
        varying vec3 vPos;
        uniform float time;
        uniform int colorMode;
        uniform int MBTI_COUNT;
        uniform vec3 mbtiDirs[16];
        uniform vec3 mbtiCols[16];
  uniform vec3 reininCols[24];
  uniform int highlightIdx;
        uniform vec3 gcNormals[4];
        uniform mat3 rotMat;
        uniform int showContours;
        uniform float contourWidth;
        uniform vec3 contourCols[4];
  uniform float contourIntensity;
        // 4D dichotomy color palette
        vec3 colorForDichotomy(vec4 d) {
          vec3 cE = vec3(0.2, 0.7, 1.0);
          vec3 cI = vec3(1.0, 0.5, 0.2);
          vec3 cS = vec3(0.3, 1.0, 0.5);
          vec3 cN = vec3(0.9, 0.2, 1.0);
          vec3 cT = vec3(1.0, 0.9, 0.2);
          vec3 cF = vec3(0.2, 1.0, 0.9);
          vec3 cJ = vec3(1.0, 0.2, 0.6);
          vec3 cP = vec3(0.2, 0.9, 1.0);
          vec3 c = d.x * cE + (1.0-d.x) * cI;
          c = mix(c, d.y * cS + (1.0-d.y) * cN, 0.5);
          c = mix(c, d.z * cT + (1.0-d.z) * cF, 0.5);
          c = mix(c, d.w * cJ + (1.0-d.w) * cP, 0.5);
          return c;
        }
        float gcLine(vec3 p, vec3 n, float w) {
          float d = abs(dot(p, n));
          float l = 1.0 - smoothstep(0.0, max(1e-5, w), d);
          return pow(l, 0.8);
        }
        int reininFaceIndex(vec3 n) {
          vec3 a = abs(n);
          int base = 0; // 0:+X,1:-X,2:+Y,3:-Y,4:+Z,5:-Z
          float m = a.x; base = (n.x >= 0.0) ? 0 : 1;
          if (a.y > m) { m = a.y; base = (n.y >= 0.0) ? 2 : 3; }
          if (a.z > m) { m = a.z; base = (n.z >= 0.0) ? 4 : 5; }
          float u = 0.0; float v = 0.0;
          if (base == 0 || base == 1) { u = n.y; v = n.z; }
          else if (base == 2 || base == 3) { u = n.x; v = n.z; }
          else { u = n.x; v = n.y; }
          float s1 = u + v;
          float s2 = u - v;
          int t = 0;
          if (s1 < 0.0 && s2 >= 0.0) t = 1;
          else if (s1 < 0.0 && s2 < 0.0) t = 2;
          else if (s1 >= 0.0 && s2 < 0.0) t = 3;
          else t = 0;
          return base * 4 + t;
        }
        void main() {
          if (colorMode == 0) {
            float x = vUv.x; float y = vUv.y; float t = time * 0.2;
            float e = smoothstep(0.0, 1.0, 0.5 + 0.5 * sin(6.2831 * (x + t)));
            float s = smoothstep(0.0, 1.0, 0.5 + 0.5 * cos(6.2831 * (y + t)));
            float tf = smoothstep(0.0, 1.0, 0.5 + 0.5 * sin(6.2831 * (x + y + t)));
            float jp = smoothstep(0.0, 1.0, 0.5 + 0.5 * cos(6.2831 * (x - y - t)));
            vec4 d = vec4(e, s, tf, jp);
            vec3 col = colorForDichotomy(d);
            float highlight = 0.15 + 0.15 * sin(12.0 * (x + y + time));
            col = mix(col, vec3(1.0), highlight);
            vec3 base = col;
            if (showContours == 1) {
              vec3 p = normalize(vPos);
              vec3 n0 = normalize(rotMat * gcNormals[0]);
              vec3 n1 = normalize(rotMat * gcNormals[1]);
              vec3 n2 = normalize(rotMat * gcNormals[2]);
              vec3 n3 = normalize(rotMat * gcNormals[3]);
              float l0 = gcLine(p, n0, contourWidth);
              float l1 = gcLine(p, n1, contourWidth);
              float l2 = gcLine(p, n2, contourWidth);
              float l3 = gcLine(p, n3, contourWidth);
              base = clamp(base + contourCols[0]*l0*contourIntensity + contourCols[1]*l1*contourIntensity + contourCols[2]*l2*contourIntensity + contourCols[3]*l3*contourIntensity, 0.0, 1.0);
            }
            gl_FragColor = vec4(base, 0.98);
          } else if (colorMode == 1) {
            vec3 nrm = normalize(vPos);
            float best = -1.0; int bestIdx = 0; float second = -1.0;
            for (int i = 0; i < 16; i++) {
              if (i >= MBTI_COUNT) break;
              float s = dot(nrm, normalize(mbtiDirs[i]));
              if (s > best) { second = best; best = s; bestIdx = i; }
              else if (s > second) { second = s; }
            }
            vec3 base = mbtiCols[bestIdx];
            float edge = clamp((best - second) * 12.0, 0.0, 1.0);
            vec3 col = mix(vec3(0.1), base, edge);
            if (showContours == 1) {
              vec3 p = nrm;
              vec3 n0 = normalize(rotMat * gcNormals[0]);
              vec3 n1 = normalize(rotMat * gcNormals[1]);
              vec3 n2 = normalize(rotMat * gcNormals[2]);
              vec3 n3 = normalize(rotMat * gcNormals[3]);
              float l0 = gcLine(p, n0, contourWidth);
              float l1 = gcLine(p, n1, contourWidth);
              float l2 = gcLine(p, n2, contourWidth);
              float l3 = gcLine(p, n3, contourWidth);
              col = clamp(col + contourCols[0]*l0*contourIntensity + contourCols[1]*l1*contourIntensity + contourCols[2]*l2*contourIntensity + contourCols[3]*l3*contourIntensity, 0.0, 1.0);
            }
            gl_FragColor = vec4(col, 0.98);
          } else {
            vec3 nrm = normalize(vPos);
            int idx = reininFaceIndex(nrm);
            vec3 col = reininCols[idx];
            if (highlightIdx >= 0) {
              if (idx == highlightIdx) {
                col = mix(col, vec3(1.0), 0.4);
              } else {
                col *= 0.35;
              }
            }
            if (showContours == 1) {
              vec3 p = nrm;
              vec3 n0 = normalize(rotMat * gcNormals[0]);
              vec3 n1 = normalize(rotMat * gcNormals[1]);
              vec3 n2 = normalize(rotMat * gcNormals[2]);
              vec3 n3 = normalize(rotMat * gcNormals[3]);
              float l0 = gcLine(p, n0, contourWidth);
              float l1 = gcLine(p, n1, contourWidth);
              float l2 = gcLine(p, n2, contourWidth);
              float l3 = gcLine(p, n3, contourWidth);
              col = clamp(col + contourCols[0]*l0*contourIntensity + contourCols[1]*l1*contourIntensity + contourCols[2]*l2*contourIntensity + contourCols[3]*l3*contourIntensity, 0.0, 1.0);
            }
            gl_FragColor = vec4(col, 0.98);
          }
        }
      `
    };
    const matDichotomy = new THREE.ShaderMaterial({
      uniforms: dichotomyShader.uniforms,
      vertexShader: dichotomyShader.vertexShader,
      fragmentShader: dichotomyShader.fragmentShader,
      transparent: true
    });
    sphere = new THREE.Mesh(geo, matDichotomy);
    scene.add(sphere);

    // Animate shader time uniform
    function animateDichotomy() {
      matDichotomy.uniforms.time.value += 0.008;
      requestAnimationFrame(animateDichotomy);
    }
    animateDichotomy();

  groupPoints = new THREE.Group();
  groupCentroids = new THREE.Group();
  groupLinks = new THREE.Group();
  groupLabels = new THREE.Group();
  groupGrid = new THREE.Group();
  groupAxis = new THREE.Group();
  scene.add(groupGrid);
  scene.add(groupAxis);
  scene.add(groupCentroids);
  scene.add(groupLinks);
  scene.add(groupLabels);
  scene.add(groupPoints);

  // Interactions
  raycaster = new THREE.Raycaster();
  mouse = new THREE.Vector2();
  // Lightweight tooltip element (disabled by default via opts.tooltip)
  tooltip = document.createElement('div');
  tooltip.style.position = 'absolute';
  tooltip.style.pointerEvents = 'none';
  tooltip.style.background = 'rgba(0,0,0,0.8)';
  tooltip.style.color = '#fff';
  tooltip.style.padding = '4px 6px';
  tooltip.style.borderRadius = '4px';
  tooltip.style.fontSize = '12px';
  tooltip.style.whiteSpace = 'nowrap';
  tooltip.style.display = 'none';
  container.style.position = container.style.position || 'relative';
  container.appendChild(tooltip);

    function animate(){
      requestAnimationFrame(animate);
      if (controls) {
        controls.target.set(0, 0, 0);
        controls.update();
      }
      renderer.render(scene, camera);
    }
    animate();

  // Initial grid render if enabled
  try { updateGrid(); } catch {}
  try { updateAxis(); } catch {}

  window.addEventListener('resize', onResize);
  function onResize(){ updateViewport(); }
  // Initial viewport setup
  updateViewport();

    // Pointer handlers
    renderer.domElement.addEventListener('mousemove', onPointerMove);
    renderer.domElement.addEventListener('mouseleave', () => { if (tooltip) tooltip.style.display = 'none'; });
  renderer.domElement.addEventListener('click', onClick);
    function onPointerMove(ev) {
      if (!opts.tooltip && currentSurfaceMode !== 2) { if (tooltip) tooltip.style.display = 'none'; return; }
      const rect = renderer.domElement.getBoundingClientRect();
      mouse.x = ((ev.clientX - rect.left) / rect.width) * 2 - 1;
      mouse.y = -((ev.clientY - rect.top) / rect.height) * 2 + 1;
      raycaster.setFromCamera(mouse, camera);
      const objs = [...pointMeshes, ...centroidMeshes];
      const hits = raycaster.intersectObjects(objs, true);
      if (hits && hits.length) {
        let obj = hits[0].object;
        while (obj && !obj.userData?.label && obj.parent) obj = obj.parent;
        const u = obj && obj.userData || {};
        let label = u.label || (typeof u.cluster === 'number' ? `Cluster ${u.cluster}` : '');
        if (u.kind === 'point' && u.mbti) {
          label = `${label} (${u.mbti}${u.inferred ? ' inferred' : ''})`;
        }
        if (label) {
          tooltip.textContent = label;
          tooltip.style.display = 'block';
          tooltip.style.left = `${ev.clientX - rect.left + 10}px`;
          tooltip.style.top = `${ev.clientY - rect.top + 10}px`;
        } else {
          tooltip.style.display = 'none';
        }
      } else {
        // If Reinin surface active, allow hovering the sphere to show face name
        if (currentSurfaceMode === 2 && sphere) {
          const shits = raycaster.intersectObject(sphere, false);
          if (shits && shits.length) {
            const hp = shits[0].point.clone().normalize();
            // Convert hit normal through polar rotation like shader does
            const m4 = new THREE.Matrix4().makeRotationFromQuaternion(polarQuat);
            hp.applyMatrix4(m4);
            const idx = jsReininFaceIndex(hp);
            const name = (Array.isArray(reininFaceNames) && reininFaceNames[idx]) ? reininFaceNames[idx] : defaultReininFaceLabel(idx);
            const txt = name || `Face ${String(idx).padStart(2,'0')}`;
            tooltip.textContent = txt;
            tooltip.style.display = 'block';
            tooltip.style.left = `${ev.clientX - rect.left + 10}px`;
            tooltip.style.top = `${ev.clientY - rect.top + 10}px`;
          } else {
            tooltip.style.display = 'none';
          }
        } else {
          tooltip.style.display = 'none';
        }
      }
    }
    function onClick(ev) {
      const rect = renderer.domElement.getBoundingClientRect();
      mouse.x = ((ev.clientX - rect.left) / rect.width) * 2 - 1;
      mouse.y = -((ev.clientY - rect.top) / rect.height) * 2 + 1;
      raycaster.setFromCamera(mouse, camera);
      const objs = [...pointMeshes, ...centroidMeshes];
      const hits = raycaster.intersectObjects(objs, true);
      if (hits && hits.length) {
        let obj = hits[0].object;
        while (obj && !obj.userData && obj.parent) obj = obj.parent;
        const u = (obj && obj.userData) || {};
        try { window.dispatchEvent(new CustomEvent('globe:select', { detail: { cid: u.cid || null, label: u.label || '', cluster: (typeof u.cluster === 'number' ? u.cluster : null), kind: u.kind || '' } })); } catch {}
      }
    }
  }

  function latLonToVec3(lat, lon, r=1.001){
    const phi = (90 - lat) * (Math.PI/180);
    const theta = (lon + 180) * (Math.PI/180);
    const x = -r * Math.sin(phi) * Math.cos(theta);
    const z = r * Math.sin(phi) * Math.sin(theta);
    const y = r * Math.cos(phi);
    return new THREE.Vector3(x,y,z);
  }

  function fibonacciSphere(n) {
    const pts = [];
    const offset = 2 / n;
    const inc = Math.PI * (3 - Math.sqrt(5));
    for (let i = 0; i < n; i++) {
      const y = ((i * offset) - 1) + (offset / 2);
      const r = Math.sqrt(1 - y*y);
      const phi = (i % n) * inc;
      const x = Math.cos(phi) * r;
      const z = Math.sin(phi) * r;
      // convert to lat/lon
      const lat = Math.asin(y) * 180/Math.PI;
      const lon = Math.atan2(z, x) * 180/Math.PI;
      pts.push({ lat, lon });
    }
    return pts;
  }

  function clearPoints(){
    // Clear points
    pointMeshes.forEach((m) => {
      try { m.geometry && m.geometry.dispose(); } catch {}
      try { m.material && m.material.dispose(); } catch {}
      try { groupPoints.remove(m); } catch {}
    });
    pointMeshes = [];
    while (groupPoints.children.length) { groupPoints.remove(groupPoints.children[0]); }
    // Clear centroids
    centroidMeshes.forEach((m) => {
      try { m.geometry && m.geometry.dispose(); } catch {}
      try { m.material && m.material.dispose(); } catch {}
      try { groupCentroids.remove(m); } catch {}
    });
    centroidMeshes = [];
    while (groupCentroids.children.length) { groupCentroids.remove(groupCentroids.children[0]); }
    // Clear links
    try {
      while (groupLinks.children.length) {
        const l = groupLinks.children.pop();
        if (l.geometry) try { l.geometry.dispose(); } catch {}
        if (l.material) try { l.material.dispose(); } catch {}
        try { groupLinks.remove(l); } catch {}
      }
    } catch {}
    // Clear labels
    try {
      while (groupLabels.children.length) {
        const s = groupLabels.children.pop();
        if (s.material && s.material.map) try { s.material.map.dispose(); } catch {}
        if (s.material) try { s.material.dispose(); } catch {}
        if (s.geometry) try { s.geometry.dispose(); } catch {}
        try { groupLabels.remove(s); } catch {}
      }
    } catch {}
    // Clear my marker
    if (myMarker) {
      try { myMarker.geometry && myMarker.geometry.dispose(); } catch {}
      try { myMarker.material && myMarker.material.dispose(); } catch {}
      try { scene.remove(myMarker); } catch {}
      myMarker = null;
    }
  }

  function setData({ centroids = [], points = [] } = {}) {
    clearPoints();
    lastData = { centroids, points };
    const sphereR = 1.0;
    const colormap = [0x1f77b4,0xff7f0e,0x2ca02c,0xd62728,0x9467bd,0x8c564b,0xe377c2,0x7f7f7f,0xbcbd22,0x17becf];
    // Draw centroids as larger spheres
    for (const c of centroids) {
      const pos = latLonToVec3(c.lat, c.lon, sphereR+0.001);
      const geo = new THREE.SphereGeometry(0.025, 16, 16);
      const mat = new THREE.MeshStandardMaterial({ color: colormap[c.id % colormap.length] });
      const mesh = new THREE.Mesh(geo, mat);
      mesh.position.copy(pos);
      const lbl = (typeof c.label === 'string' && c.label) ? c.label : `Cluster ${c.id}`;
      mesh.userData = { label: lbl, cluster: c.id, kind: 'centroid' };
      groupCentroids.add(mesh);
      centroidMeshes.push(mesh);
    }
    // Draw points as small spheres
    for (const p of points) {
      const pos = latLonToVec3(p.lat, p.lon, sphereR+0.002);
      const geo = new THREE.SphereGeometry(0.012, 12, 12);
      const mat = new THREE.MeshStandardMaterial({ color: colormap[p.cluster % colormap.length] });
      const mesh = new THREE.Mesh(geo, mat);
      mesh.position.copy(pos);
  mesh.userData = { cid: p.cid, label: p.label, mbti: p.mbti, inferred: !!p.inferred, cluster: p.cluster, kind: 'point' };
      groupPoints.add(mesh);
      pointMeshes.push(mesh);
    }
  // Apply current options (links/labels)
  try { updateLinks(); } catch {}
  try { updateLabels(); } catch {}
  }

  function highlightCluster(idx) {
    try {
      const colormap = [0x1f77b4,0xff7f0e,0x2ca02c,0xd62728,0x9467bd,0x8c564b,0xe377c2,0x7f7f7f,0xbcbd22,0x17becf];
      // Dim all
      for (const m of pointMeshes) { if (m && m.material) m.material.opacity = 0.35, m.material.transparent = true; }
      for (const m of centroidMeshes) { if (m && m.material) m.material.opacity = 0.35, m.material.transparent = true; }
      // Highlight selected
      for (const m of pointMeshes) {
        const c = (m.userData && typeof m.userData.cluster === 'number') ? m.userData.cluster : -1;
        if (c === idx && m.material) { m.material.opacity = 1.0; m.material.transparent = false; m.material.color.setHex(colormap[idx % colormap.length]); }
      }
      for (const m of centroidMeshes) {
        const c = (m.userData && typeof m.userData.cluster === 'number') ? m.userData.cluster : -1;
        if (c === idx && m.material) { m.material.opacity = 1.0; m.material.transparent = false; m.material.color.setHex(colormap[idx % colormap.length]); }
      }
    } catch {}
  }

  function setMyMarker({ lat, lon, label = 'Me' }) {
    try {
      // Remove existing
      if (myMarker) {
        try { scene.remove(myMarker); } catch {}
        try { myMarker.geometry && myMarker.geometry.dispose(); } catch {}
        try { myMarker.material && myMarker.material.dispose(); } catch {}
        myMarker = null;
      }
      const pos = latLonToVec3(lat, lon, 1.005);
      const geo = new THREE.SphereGeometry(0.018, 16, 16);
      const mat = new THREE.MeshStandardMaterial({ color: 0xe74c3c, emissive: 0x220000, metalness: 0.2, roughness: 0.4 });
      myMarker = new THREE.Mesh(geo, mat);
      myMarker.position.copy(pos);
      myMarker.userData = { label };
      scene.add(myMarker);
    } catch {}
  }

  function pulseCid(cid) {
    try {
      if (!cid) return;
      const m = pointMeshes.find((x) => x && x.userData && x.userData.cid === cid);
      if (!m) return;
      const orig = {
        scale: m.scale.clone(),
        color: m.material && m.material.color ? m.material.color.clone() : null,
        emissive: m.material && m.material.emissive ? m.material.emissive.clone() : null,
        emissiveIntensity: (m.material && typeof m.material.emissiveIntensity === 'number') ? m.material.emissiveIntensity : undefined
      };
      try {
        m.scale.setScalar(1.8);
        if (m.material) {
          if (m.material.emissive) m.material.emissive.setHex(0xffffff);
          if (typeof m.material.emissiveIntensity === 'number') m.material.emissiveIntensity = 0.6;
        }
      } catch {}
      setTimeout(() => {
        try {
          if (orig.scale) m.scale.copy(orig.scale);
          if (m.material) {
            if (orig.color) m.material.color.copy(orig.color);
            if (orig.emissive) m.material.emissive.copy(orig.emissive);
            if (typeof orig.emissiveIntensity === 'number') m.material.emissiveIntensity = orig.emissiveIntensity;
          }
        } catch {}
      }, 700);
    } catch {}
  }

  function updateLinks() {
    // Links from centroid to each point, throttled for perf
    try {
      while (groupLinks.children.length) {
        const l = groupLinks.children.pop();
        if (l.geometry) try { l.geometry.dispose(); } catch {}
        if (l.material) try { l.material.dispose(); } catch {}
      }
      if (!opts.links) return;
      const maxLinks = Math.min(1500, lastData.points.length);
      const centroidPos = new Map();
      for (const c of lastData.centroids) centroidPos.set(c.id, latLonToVec3(c.lat, c.lon, 1.001));
      const mat = new THREE.LineBasicMaterial({ color: 0x888888, transparent: true, opacity: 0.35 });
      for (let i = 0; i < maxLinks; i++) {
        const p = lastData.points[i];
        if (!p) continue;
        const cp = centroidPos.get(p.cluster);
        if (!cp) continue;
        const pp = latLonToVec3(p.lat, p.lon, 1.002);
        const geo = new THREE.BufferGeometry().setFromPoints([cp, pp]);
        const line = new THREE.Line(geo, mat);
        groupLinks.add(line);
      }
    } catch {}
  }

  function makeLabelSprite(text, color = '#fff') {
    const pad = 4; const fs = 14; const font = `${fs}px sans-serif`;
    const ctxCanvas = document.createElement('canvas');
    const ctx = ctxCanvas.getContext('2d');
    ctx.font = font;
    const w = Math.ceil(ctx.measureText(text).width) + pad * 2;
    const h = fs + pad * 2;
    ctxCanvas.width = w; ctxCanvas.height = h;
    const ctx2 = ctxCanvas.getContext('2d');
    ctx2.font = font;
    ctx2.fillStyle = 'rgba(0,0,0,0.6)';
    ctx2.fillRect(0, 0, w, h);
    ctx2.fillStyle = color;
    ctx2.textBaseline = 'middle';
    ctx2.fillText(text, pad, h / 2);
    const tex = new THREE.CanvasTexture(ctxCanvas);
    const mat = new THREE.SpriteMaterial({ map: tex, transparent: true });
    const spr = new THREE.Sprite(mat);
    const scale = 0.25; // world units
    spr.scale.set(scale, scale * (h / w), 1);
    return spr;
  }

  function updateLabels() {
    try {
      while (groupLabels.children.length) {
        const s = groupLabels.children.pop();
        if (s.material && s.material.map) try { s.material.map.dispose(); } catch {}
        if (s.material) try { s.material.dispose(); } catch {}
        if (s.geometry) try { s.geometry.dispose(); } catch {}
      }
      if (!opts.labels) return;
      for (const c of lastData.centroids) {
        const pos = latLonToVec3(c.lat, c.lon, 1.03);
        const text = (typeof c.label === 'string' && c.label) ? c.label : `Cluster ${c.id}`;
        const spr = makeLabelSprite(text, '#ffffff');
        spr.position.copy(pos);
        groupLabels.add(spr);
      }
    } catch {}
  }

  function updateGrid() {
    try {
      // clear prior grid
      while (groupGrid && groupGrid.children && groupGrid.children.length) {
        const g = groupGrid.children.pop();
        if (g.geometry) try { g.geometry.dispose(); } catch {}
        if (g.material) try { g.material.dispose(); } catch {}
      }
      if (!opts.grid || !groupGrid) return;
  // Vibrant, visible contour lines
  const matEq = new THREE.LineBasicMaterial({ color: 0xffe119, transparent: false, linewidth: 2 });
  const matMer = new THREE.LineBasicMaterial({ color: 0xe6194b, transparent: false, linewidth: 2 });
      const R = 1.001;
      // Equator and tropics/parallels every 30 degrees
      const latSteps = [ -60, -30, 0, 30, 60 ];
      for (const lat of latSteps) {
        const pts = [];
        for (let lon = -180; lon <= 180; lon += 3) {
          const v = latLonToVec3(lat, lon, R);
          pts.push(v);
        }
        const geo = new THREE.BufferGeometry().setFromPoints(pts);
        const line = new THREE.Line(geo, lat === 0 ? matEq : matMer);
        groupGrid.add(line);
      }
      // Meridians every 30 degrees
      for (let lon = -180; lon < 180; lon += 30) {
        const pts = [];
        for (let lat = -90; lat <= 90; lat += 3) {
          const v = latLonToVec3(lat, lon, R);
          pts.push(v);
        }
        const geo = new THREE.BufferGeometry().setFromPoints(pts);
        const line = new THREE.Line(geo, matMer);
        groupGrid.add(line);
      }
    } catch {}
  }

  function updateAxis() {
    try {
      while (groupAxis && groupAxis.children && groupAxis.children.length) {
        const a = groupAxis.children.pop();
        if (a.geometry) try { a.geometry.dispose(); } catch {}
        if (a.material) try { a.material.dispose(); } catch {}
      }
      // Visualize current polar axis (rotated z)
      const matPos = new THREE.LineBasicMaterial({ color: 0xffffff, transparent: true, opacity: 0.9 });
      const matNeg = new THREE.LineBasicMaterial({ color: 0x444444, transparent: true, opacity: 0.9 });
      const axis = new THREE.Vector3(0,0,1).applyQuaternion(polarQuat).normalize();
      const p1 = axis.clone().multiplyScalar(1.05);
      const p2 = axis.clone().multiplyScalar(-1.05);
      const g1 = new THREE.BufferGeometry().setFromPoints([p2, p1]);
      groupAxis.add(new THREE.Line(g1, matPos));
      // Arrowheads
      const headLen = 0.06;
      const ortho = Math.abs(axis.x) > 0.9 ? new THREE.Vector3(0,1,0) : new THREE.Vector3(1,0,0);
      const t1 = new THREE.Vector3().crossVectors(axis, ortho).normalize();
      const t2 = new THREE.Vector3().crossVectors(axis, t1).normalize();
      const h1 = p1.clone().add(t1.clone().multiplyScalar(-headLen));
      const h2 = p1.clone().add(t2.clone().multiplyScalar(-headLen));
      groupAxis.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints([p1, h1]), matPos));
      groupAxis.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints([p1, h2]), matPos));
      const h3 = p2.clone().add(t1.clone().multiplyScalar(headLen));
      const h4 = p2.clone().add(t2.clone().multiplyScalar(headLen));
      groupAxis.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints([p2, h3]), matNeg));
      groupAxis.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints([p2, h4]), matNeg));
    } catch {}
  }

  function setOptions(o = {}) {
  opts = { ...opts, ...(o || {}) };
  // Hide tooltip if disabled
  if (tooltip && !opts.tooltip) tooltip.style.display = 'none';
    updateGrid();
    updateLinks();
    updateLabels();
  try { if (groupAxis) groupAxis.visible = true; } catch {}
  }

  function setSurfaceMode(mode) {
    try {
  const m = (mode === 'mbti') ? 1 : (mode === 'reinin' ? 2 : 0);
      if (sphere && sphere.material && sphere.material.uniforms && sphere.material.uniforms.colorMode) {
        sphere.material.uniforms.colorMode.value = m;
      }
      currentSurfaceMode = m;
    } catch {}
  }

  function setSurfaceMBTI({ dirs = [], colors = [] } = {}) {
    try {
      const vecs = (dirs || []).slice(0,16).map((v) => new THREE.Vector3(v[0]||0, v[1]||0, v[2]||0));
      const cols = (colors || []).slice(0,16).map((c) => new THREE.Vector3(c[0]||1, c[1]||1, c[2]||1));
      while (vecs.length < 16) vecs.push(new THREE.Vector3(1,0,0));
      while (cols.length < 16) cols.push(new THREE.Vector3(1,1,1));
      if (sphere && sphere.material && sphere.material.uniforms) {
        sphere.material.uniforms.mbtiDirs.value = vecs;
        sphere.material.uniforms.mbtiCols.value = cols;
        if (sphere.material.uniforms.MBTI_COUNT) sphere.material.uniforms.MBTI_COUNT.value = Math.min(dirs.length || 16, 16);
        applyContourUniforms();
      }
    } catch {}
  }

  function setSurfaceReinin({ colors = [] } = {}) {
    try {
      const cols = (colors || []).slice(0,24).map((c) => new THREE.Vector3(c[0]||0.5, c[1]||0.5, c[2]||0.5));
      while (cols.length < 24) cols.push(new THREE.Vector3(0.5,0.5,0.5));
      reininColors = cols.map(v => new THREE.Color(v.x, v.y, v.z));
      if (sphere && sphere.material && sphere.material.uniforms) {
        sphere.material.uniforms.reininCols.value = cols;
        applyContourUniforms();
      }
    } catch {}
  }

  function setReininHighlight(i = -1) {
    try {
      if (!sphere || !sphere.material || !sphere.material.uniforms) return;
      const u = sphere.material.uniforms;
      if (u.highlightIdx) u.highlightIdx.value = (typeof i === 'number' ? i|0 : -1);
    } catch {}
  }

  function setReininFaceNames(names = []){
    try {
      if (!Array.isArray(names) || names.length !== 24) { reininFaceNames = null; return; }
      reininFaceNames = names.map(x => String(x||''));
    } catch {}
  }

  function applyContourUniforms() {
    try {
      if (!sphere || !sphere.material || !sphere.material.uniforms) return;
      const u = sphere.material.uniforms;
      u.gcNormals.value = circleNormals.map(v => v.clone());
      const m4 = new THREE.Matrix4().makeRotationFromQuaternion(polarQuat);
      const m3 = new THREE.Matrix3().setFromMatrix4(m4);
      u.rotMat.value = m3;
      u.showContours.value = contourEnabled ? 1 : 0;
      u.contourWidth.value = contourWidth;
      u.contourCols.value = contourColors.map(c => new THREE.Vector3(c.r, c.g, c.b));
    u.contourIntensity.value = contourIntensity;
    } catch {}
  }

  function setGreatCircles({ normals, colors, width, enabled, intensity } = {}) {
    try {
      if (Array.isArray(normals) && normals.length >= 4) {
        circleNormals = normals.slice(0,4).map(n => new THREE.Vector3(n[0]||0, n[1]||0, n[2]||0).normalize());
      }
      if (Array.isArray(colors) && colors.length >= 4) {
        contourColors = colors.slice(0,4).map(c => new THREE.Color(c[0]||1, c[1]||1, c[2]||1));
      }
      if (typeof width === 'number' && isFinite(width)) contourWidth = Math.max(0.005, Math.min(width, 0.5));
      if (typeof enabled === 'boolean') contourEnabled = enabled;
    if (typeof intensity === 'number' && isFinite(intensity)) contourIntensity = Math.max(0.0, Math.min(intensity, 2.0));
      applyContourUniforms();
    } catch {}
  }

  function setPolarRotation({ axis = [0,1,0], angle = 0 } = {}) {
    try {
      const ax = new THREE.Vector3(axis[0]||0, axis[1]||0, axis[2]||0).normalize();
      polarQuat.setFromAxisAngle(ax, angle);
      applyContourUniforms();
      updateAxis();
    } catch {}
  }

  // Convert Reinin face index (0..23) to a representative unit direction vector
  function faceIndexToDirection(i){
    const idx = Math.max(0, Math.min(23, i|0));
    const base = Math.floor(idx / 4); // 0:+X,1:-X,2:+Y,3:-Y,4:+Z,5:-Z
    const quad = idx % 4;            // 0..3
    // Choose u,v pairs satisfying quadrant conditions and avoid edges by scaling
    let u=0, v=0;
    const mag = 0.5; // side component magnitude
    if (quad === 0) { u = +mag; v = 0; }         // s1>=0 & s2>=0
    else if (quad === 1) { u = 0; v = -mag; }    // s1<0 & s2>=0
    else if (quad === 2) { u = -mag; v = 0; }    // s1<0 & s2<0
    else { u = 0; v = +mag; }                    // s1>=0 & s2<0
    // Build vector per base axis mapping used in shader for (u,v)
    let x=0, y=0, z=0;
    const main = 1.0; // dominant axis magnitude
    if (base === 0) { x = +main; y = u; z = v; }     // +X: u=y, v=z
    else if (base === 1) { x = -main; y = u; z = v; }// -X
    else if (base === 2) { y = +main; x = u; z = v; }// +Y: u=x, v=z
    else if (base === 3) { y = -main; x = u; z = v; }// -Y
    else if (base === 4) { z = +main; x = u; y = v; }// +Z: u=x, v=y
    else { z = -main; x = u; y = v; }                // -Z
    const v3 = new THREE.Vector3(x,y,z).normalize();
    return v3;
  }

  // Smoothly rotate camera to center the given Reinin face
  let camTween = null;
  function focusReininFace(i, animate=true){
    try {
      if (!camera) return;
      const dir = faceIndexToDirection(i);
      // Account for polar rotation (shader rotates normals via rotMat); apply same to dir for consistency
      const m4 = new THREE.Matrix4().makeRotationFromQuaternion(polarQuat);
      dir.applyMatrix4(m4);
      const r = camera.position.length() || 3.2;
      const targetPos = dir.clone().multiplyScalar(-r);
      if (!animate) {
        camera.position.copy(targetPos);
        camera.lookAt(0,0,0);
        return;
      }
      // Cancel previous tween
      if (camTween && camTween.stop) { try { camTween.stop = true; } catch {} }
      const start = performance.now();
      const dur = 500;
      const from = camera.position.clone();
      camTween = { stop: false };
      const ease = (t)=> t<0.5 ? 2*t*t : 1 - Math.pow(-2*t+2,2)/2; // easeInOutQuad
      function step(now){
        if (!camTween || camTween.stop) return;
        const t = Math.min(1, (now - start) / dur);
        const k = ease(t);
        camera.position.lerpVectors(from, targetPos, k);
        camera.lookAt(0,0,0);
        if (t < 1) requestAnimationFrame(step);
      }
      requestAnimationFrame(step);
    } catch {}
  }

  // JS version of shader's reininFaceIndex
  function jsReininFaceIndex(n){
    const a = new THREE.Vector3(Math.abs(n.x), Math.abs(n.y), Math.abs(n.z));
    let base = 0; // 0:+X,1:-X,2:+Y,3:-Y,4:+Z,5:-Z
    let m = a.x; base = (n.x >= 0) ? 0 : 1;
    if (a.y > m) { m = a.y; base = (n.y >= 0) ? 2 : 3; }
    if (a.z > m) { m = a.z; base = (n.z >= 0) ? 4 : 5; }
    let u=0, v=0;
    if (base === 0 || base === 1) { u = n.y; v = n.z; }
    else if (base === 2 || base === 3) { u = n.x; v = n.z; }
    else { u = n.x; v = n.y; }
    const s1 = u + v; const s2 = u - v;
    let t = 0;
    if (s1 < 0 && s2 >= 0) t = 1;
    else if (s1 < 0 && s2 < 0) t = 2;
    else if (s1 >= 0 && s2 < 0) t = 3;
    else t = 0;
    return base * 4 + t;
  }

  // Given a Reinin face index (0..23), return its opposite face index (same tri on opposite cube face)
  function oppositeReininFaceIndex(i){
    const idx = Math.max(0, Math.min(23, i|0));
    const base = Math.floor(idx / 4); // 0:+X,1:-X,2:+Y,3:-Y,4:+Z,5:-Z
    const tri = idx % 4;
    // Opposites are paired (+/-) by flipping lowest bit within each axis pair
    const baseOpp = (base ^ 1);
    return baseOpp * 4 + tri;
  }

  // Dev helper: quick self-test of classifier symmetry and coverage
  function testReininClassifier(){
    try {
      // 1) Cardinal directions should map to base faces with tri=0 by construction
      const dirs = [
        new THREE.Vector3(1,0,0), new THREE.Vector3(-1,0,0),
        new THREE.Vector3(0,1,0), new THREE.Vector3(0,-1,0),
        new THREE.Vector3(0,0,1), new THREE.Vector3(0,0,-1)
      ];
      const expectedBase = [0,1,2,3,4,5];
      let ok = true; const notes = [];
      for (let k = 0; k < 6; k++) {
        const id = jsReininFaceIndex(dirs[k]);
        const base = Math.floor(id / 4), tri = id % 4;
        if (base !== expectedBase[k] || tri !== 0) { ok = false; notes.push(`cardinal mismatch k=${k} id=${id} base=${base} tri=${tri}`); }
      }
      // 2) Opposite mapping consistency using representatives
      for (let i = 0; i < 24; i++) {
        const opp = oppositeReininFaceIndex(i);
        if (oppositeReininFaceIndex(opp) !== i) { ok = false; notes.push(`opp not involution at ${i}`); }
        // Representative vector should map back to its own id
        const v = faceIndexToDirection(i);
        const id2 = jsReininFaceIndex(v);
        if (id2 !== i) { ok = false; notes.push(`rep mismatch at ${i} -> ${id2}`); }
      }
      return { ok, notes };
    } catch (e) { return { ok: false, error: String(e) }; }
  }

  function defaultReininFaceLabel(i){
    const idx = Math.max(0, Math.min(23, i|0));
    return `Face ${String(idx).padStart(2,'0')}`;
  }

  function dispose(){
    try { window.removeEventListener('resize', null); } catch {}
    if (!renderer) return;
    try { renderer.domElement.removeEventListener('mousemove', null); } catch {}
    clearPoints();
    sphere && sphere.geometry && sphere.geometry.dispose();
    sphere && sphere.material && sphere.material.dispose();
    if (tooltip && tooltip.parentNode) { try { tooltip.parentNode.removeChild(tooltip); } catch {} }
    renderer.dispose();
    if (renderer.domElement && renderer.domElement.parentNode) renderer.domElement.parentNode.removeChild(renderer.domElement);
    scene = camera = renderer = sphere = null;
  }

  return { init, setData, dispose, highlightCluster, setMyMarker, pulseCid, setOptions, setSurfaceMode, setSurfaceMBTI, setSurfaceReinin, setGreatCircles, setPolarRotation, setReininHighlight, focusReininFace, setReininFaceNames, 
    // programmatic helpers
    reininFaceIndex: jsReininFaceIndex, faceIndexToDirection,
    // debug helpers
    _testReinin: testReininClassifier, _oppositeReinin: oppositeReininFaceIndex };
})();

window.GLOBE = GLOBE;
