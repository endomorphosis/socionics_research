// Three.js MBTI sphere with great circles and 16 markers
// API: createMBTISphere(container, options?) => { setOptions, dispose }

import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { MBTI_TYPES, TYPE_PREFS, FUNCTION_STACKS, DEFAULT_NORMALS, normalize, add, scale, dot } from './mbti_data.js';

export function createMBTISphere(container, opts = {}) {
  const options = {
    showCircles: true,
    showRegions: true,
    showLabels: true,
    circleThickness: 0.02,
    regionAlpha: 0.18,
    ...opts
  };

  // Renderer with square viewport + scissor for perfect circle
  const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
  renderer.setPixelRatio(Math.min(2, window.devicePixelRatio || 1));
  container.appendChild(renderer.domElement);

  const scene = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(45, 1, 0.1, 100);
  camera.position.set(0, 0, 3.2);

  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true;

  const light = new THREE.DirectionalLight(0xffffff, 1.0);
  light.position.set(3, 3, 5);
  scene.add(light);
  scene.add(new THREE.AmbientLight(0xffffff, 0.45));

  // Great-circle normals
  const normals = { ...DEFAULT_NORMALS };

  // Sphere surface material: shader draws circles and subtle regions
  const uniforms = {
    nE: { value: new THREE.Vector3(...normals.nE) },
    nN: { value: new THREE.Vector3(...normals.nN) },
    nF: { value: new THREE.Vector3(...normals.nF) },
    nP: { value: new THREE.Vector3(...normals.nP) },
    showCircles: { value: options.showCircles ? 1 : 0 },
    showRegions: { value: options.showRegions ? 1 : 0 },
    circleThickness: { value: options.circleThickness },
    regionAlpha: { value: options.regionAlpha }
  };

  const material = new THREE.ShaderMaterial({
    uniforms,
    vertexShader: `
      varying vec3 vN;
      void main(){
        vec4 wp = modelMatrix * vec4(position,1.0);
        vN = normalize(normalMatrix * normal);
        gl_Position = projectionMatrix * viewMatrix * wp;
      }
    `,
    fragmentShader: `
      precision highp float;
      varying vec3 vN;
      uniform vec3 nE, nN, nF, nP;
      uniform int showCircles, showRegions;
      uniform float circleThickness;
      uniform float regionAlpha;
      
      float linstep(float a, float b, float x){ return clamp((x-a)/(b-a), 0.0, 1.0); }
      vec3 circleColor(vec3 n){
        // Map each normal to a fixed color for visibility
        if (n == nE) return vec3(1.0,0.2,0.2);
        if (n == nN) return vec3(0.8,0.4,1.0);
        if (n == nF) return vec3(0.2,1.0,0.4);
        return vec3(0.9,0.9,0.2);
      }
      
      vec3 regionColor(vec3 p){
        // Compute 4-bit key; simple hash to RGB
        int sE = (dot(nE,p) >= 0.0) ? 1 : 0;
        int sN = (dot(nN,p) >= 0.0) ? 1 : 0;
        int sF = (dot(nF,p) >= 0.0) ? 1 : 0;
        int sP = (dot(nP,p) >= 0.0) ? 1 : 0;
        int key = (sE<<3) | (sN<<2) | (sF<<1) | sP;
        // Hash to color
        float r = float((key * 97) % 255) / 255.0;
        float g = float((key * 57) % 255) / 255.0;
        float b = float((key * 31) % 255) / 255.0;
        return vec3(r,g,b);
      }
      
      void main(){
        vec3 p = normalize(vN); // on unit sphere
        vec3 base = vec3(0.02,0.02,0.04);
        vec3 col = base;
        
        if (showRegions == 1){
          vec3 rc = regionColor(p);
          col = mix(col, rc, regionAlpha);
        }
        if (showCircles == 1){
          float dE = abs(dot(nE,p));
          float dN = abs(dot(nN,p));
          float dF = abs(dot(nF,p));
          float dP = abs(dot(nP,p));
          float t = circleThickness;
          float aE = 1.0 - linstep(t, t*2.0, dE);
          float aN = 1.0 - linstep(t, t*2.0, dN);
          float aF = 1.0 - linstep(t, t*2.0, dF);
          float aP = 1.0 - linstep(t, t*2.0, dP);
          col = mix(col, vec3(1.0,0.2,0.2), aE);
          col = mix(col, vec3(0.8,0.4,1.0), aN);
          col = mix(col, vec3(0.2,1.0,0.4), aF);
          col = mix(col, vec3(0.9,0.9,0.2), aP);
        }
        gl_FragColor = vec4(col, 1.0);
      }
    `,
    lights: false,
    transparent: false
  });

  const sphere = new THREE.Mesh(new THREE.SphereGeometry(1, 128, 128), material);
  scene.add(sphere);

  // Markers
  const markerGeo = new THREE.SphereGeometry(0.02, 16, 16);
  const labelCanvas = document.createElement('canvas');
  const ctx = labelCanvas.getContext('2d');

  function markerColorFor(type){
    // Simple stable color by string hash
    let h = 0; for (let i = 0; i < type.length; i++) h = (h*31 + type.charCodeAt(i))|0;
    const r = ((h>>>0) % 255)/255, g = ((h>>>8) % 255)/255, b=((h>>>16)%255)/255;
    return new THREE.Color(r,g,b);
  }

  function computeBaseDir(prefs){
    const wE=1, wN=1, wF=1, wP=1;
    const sE = prefs.E ? 1 : -1;
    const sN = prefs.N ? 1 : -1;
    const sF = prefs.F ? 1 : -1;
    const sP = prefs.P ? 1 : -1;
    const v = add(add(scale(normals.nE, wE*sE), scale(normals.nN, wN*sN)), add(scale(normals.nF, wF*sF), scale(normals.nP, wP*sP)));
    return normalize(v);
  }

  function placeType(type){
    const prefs = TYPE_PREFS[type];
    const v0 = computeBaseDir(prefs);
    const mesh = new THREE.Mesh(markerGeo, new THREE.MeshBasicMaterial({ color: markerColorFor(type) }));
    mesh.position.set(v0[0]*1.001, v0[1]*1.001, v0[2]*1.001);
    mesh.userData = { type, stack: FUNCTION_STACKS[type] };
    scene.add(mesh);
    // Minimal text sprite label
    const label = makeTextSprite(type);
    label.position.copy(mesh.position).multiplyScalar(1.02);
    scene.add(label);
    return { mesh, label };
  }

  function makeTextSprite(text){
    const fontSize = 64;
    const pad = 16;
    const font = `${fontSize}px sans-serif`;
    const c = document.createElement('canvas');
    const g = c.getContext('2d');
    g.font = font;
    const m = g.measureText(text);
    c.width = Math.ceil(m.width + pad*2);
    c.height = Math.ceil(fontSize + pad*2);
    g.font = font;
    g.textBaseline = 'top';
    g.fillStyle = 'white';
    g.strokeStyle = 'black';
    g.lineWidth = 6;
    g.strokeText(text, pad, pad);
    g.fillText(text, pad, pad);
    const tex = new THREE.CanvasTexture(c);
    tex.anisotropy = 4;
    const mat = new THREE.SpriteMaterial({ map: tex, depthTest: true, depthWrite: false, transparent: true });
    const spr = new THREE.Sprite(mat);
    const scaleK = 0.0025; // adjust label size
    spr.scale.set(c.width*scaleK, c.height*scaleK, 1);
    return spr;
  }

  const markers = MBTI_TYPES.map(placeType);

  // Resize handling (square viewport)
  function resize(){
    const { clientWidth: W, clientHeight: H } = container;
    const S = Math.min(W, H);
    const x = (W - S) >> 1; const y = (H - S) >> 1;
    renderer.setSize(W, H, false);
    renderer.setViewport(x, y, S, S);
    renderer.setScissor(x, y, S, S);
    renderer.setScissorTest(true);
    camera.aspect = 1;
    camera.updateProjectionMatrix();
  }
  resize();
  window.addEventListener('resize', resize);

  // Simple raycaster hover tooltip (browser title)
  const ray = new THREE.Raycaster();
  const mouse = new THREE.Vector2();
  renderer.domElement.addEventListener('mousemove', (e) => {
    const rect = renderer.domElement.getBoundingClientRect();
    mouse.x = ((e.clientX - rect.left) / rect.width) * 2 - 1;
    mouse.y = -((e.clientY - rect.top) / rect.height) * 2 + 1;
  });

  function animate(){
    controls.update();
    ray.setFromCamera(mouse, camera);
    const intersects = ray.intersectObjects(scene.children, true);
    const m = intersects.find(i => i.object && i.object.geometry === markerGeo);
    if (m && m.object && m.object.userData && m.object.userData.type) {
      const t = m.object.userData.type;
      const s = m.object.userData.stack;
      renderer.domElement.title = `${t}  [${s.join(', ')}]`;
    } else {
      renderer.domElement.title = '';
    }
    renderer.render(scene, camera);
    requestAnimationFrame(animate);
  }
  requestAnimationFrame(animate);

  function setOptions(p){
    if (!p) return;
    Object.assign(options, p);
    uniforms.showCircles.value = options.showCircles ? 1 : 0;
    uniforms.showRegions.value = options.showRegions ? 1 : 0;
    uniforms.circleThickness.value = options.circleThickness;
    uniforms.regionAlpha.value = options.regionAlpha;
  }

  function dispose(){
    window.removeEventListener('resize', resize);
    renderer.dispose();
    try { container.removeChild(renderer.domElement); } catch {}
  }

  return { setOptions, dispose };
}
