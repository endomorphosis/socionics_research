// Minimal globe renderer using Three.js for the Personality Planet
import * as THREE from 'three';

const GLOBE = (() => {
  let scene, camera, renderer, sphere, container;
  let groupPoints, groupCentroids, groupLinks, groupLabels;
  let pointMeshes = [], centroidMeshes = [];
  let myMarker = null;
  let raycaster, mouse, tooltip;
  let lastData = { centroids: [], points: [] };
  let opts = { links: false, labels: false };

  function init(containerEl) {
    container = containerEl;
    const w = container.clientWidth || 800;
    const h = container.clientHeight || 600;
    scene = new THREE.Scene();
    camera = new THREE.PerspectiveCamera(50, w/h, 0.1, 1000);
    camera.position.set(0, 0, 3.2);

  renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setSize(w, h);
    container.appendChild(renderer.domElement);

    const amb = new THREE.AmbientLight(0xffffff, 0.9);
    scene.add(amb);
    const dir = new THREE.DirectionalLight(0xffffff, 0.4);
    dir.position.set(5, 3, 2);
    scene.add(dir);

    const geo = new THREE.SphereGeometry(1, 64, 64);
    const mat = new THREE.MeshStandardMaterial({ color: 0x0f2747, metalness: 0.1, roughness: 0.7, transparent: true, opacity: 0.95 });
    sphere = new THREE.Mesh(geo, mat);
    scene.add(sphere);

  groupPoints = new THREE.Group();
  groupCentroids = new THREE.Group();
  groupLinks = new THREE.Group();
  groupLabels = new THREE.Group();
  scene.add(groupCentroids);
  scene.add(groupLinks);
  scene.add(groupLabels);
  scene.add(groupPoints);

  // Interactions
  raycaster = new THREE.Raycaster();
  mouse = new THREE.Vector2();
  // Lightweight tooltip element
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

    function animate(){ requestAnimationFrame(animate); renderer.render(scene, camera); }
    animate();

    window.addEventListener('resize', onResize);
    function onResize(){
      const w2 = container.clientWidth || w; const h2 = container.clientHeight || h;
      camera.aspect = w2/h2; camera.updateProjectionMatrix(); renderer.setSize(w2,h2);
    }

    // Pointer handlers
    renderer.domElement.addEventListener('mousemove', onPointerMove);
    renderer.domElement.addEventListener('mouseleave', () => { if (tooltip) tooltip.style.display = 'none'; });
  renderer.domElement.addEventListener('click', onClick);
    function onPointerMove(ev) {
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
        const label = u.label || (typeof u.cluster === 'number' ? `Cluster ${u.cluster}` : '');
        if (label) {
          tooltip.textContent = label;
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
      mesh.userData = { label: `Cluster ${c.id}`, cluster: c.id, kind: 'centroid' };
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
      mesh.userData = { cid: p.cid, label: p.label, cluster: p.cluster, kind: 'point' };
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
        const spr = makeLabelSprite(`Cluster ${c.id}`, '#ffffff');
        spr.position.copy(pos);
        groupLabels.add(spr);
      }
    } catch {}
  }

  function setOptions(o = {}) {
    opts = { ...opts, ...(o || {}) };
    updateLinks();
    updateLabels();
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

  return { init, setData, dispose, highlightCluster, setMyMarker, pulseCid, setOptions };
})();

window.GLOBE = GLOBE;
