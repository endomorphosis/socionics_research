// MBTI function stacks and function-aware placement helpers
// Exports:
// - getStack(type)
// - computeFunctionOffset(p, type, normals, weights, attitudeRollRad, offsetScale, maxDeg)
// - DEFAULT_ROLE_WEIGHTS
// - TETRAHEDRAL_NORMALS

// Function stacks (role order: dominant, auxiliary, tertiary, inferior)
// Each entry: { f: 'Ni'|'Te'|..., att: 'intro'|'extra', axis: 'N'|'F' } axis determines which dichotomy plane it influences primarily
const STACKS = {
  ISTJ: [ {f:'Si',att:'intro',axis:'N'}, {f:'Te',att:'extra',axis:'F'}, {f:'Fi',att:'intro',axis:'F'}, {f:'Ne',att:'extra',axis:'N'} ],
  ISFJ: [ {f:'Si',att:'intro',axis:'N'}, {f:'Fe',att:'extra',axis:'F'}, {f:'Ti',att:'intro',axis:'F'}, {f:'Ne',att:'extra',axis:'N'} ],
  INFJ: [ {f:'Ni',att:'intro',axis:'N'}, {f:'Fe',att:'extra',axis:'F'}, {f:'Ti',att:'intro',axis:'F'}, {f:'Se',att:'extra',axis:'N'} ],
  INTJ: [ {f:'Ni',att:'intro',axis:'N'}, {f:'Te',att:'extra',axis:'F'}, {f:'Fi',att:'intro',axis:'F'}, {f:'Se',att:'extra',axis:'N'} ],
  ISTP: [ {f:'Ti',att:'intro',axis:'F'}, {f:'Se',att:'extra',axis:'N'}, {f:'Ni',att:'intro',axis:'N'}, {f:'Fe',att:'extra',axis:'F'} ],
  ISFP: [ {f:'Fi',att:'intro',axis:'F'}, {f:'Se',att:'extra',axis:'N'}, {f:'Ni',att:'intro',axis:'N'}, {f:'Te',att:'extra',axis:'F'} ],
  INFP: [ {f:'Fi',att:'intro',axis:'F'}, {f:'Ne',att:'extra',axis:'N'}, {f:'Si',att:'intro',axis:'N'}, {f:'Te',att:'extra',axis:'F'} ],
  INTP: [ {f:'Ti',att:'intro',axis:'F'}, {f:'Ne',att:'extra',axis:'N'}, {f:'Si',att:'intro',axis:'N'}, {f:'Fe',att:'extra',axis:'F'} ],
  ESTP: [ {f:'Se',att:'extra',axis:'N'}, {f:'Ti',att:'intro',axis:'F'}, {f:'Fe',att:'extra',axis:'F'}, {f:'Ni',att:'intro',axis:'N'} ],
  ESFP: [ {f:'Se',att:'extra',axis:'N'}, {f:'Fi',att:'intro',axis:'F'}, {f:'Te',att:'extra',axis:'F'}, {f:'Ni',att:'intro',axis:'N'} ],
  ENFP: [ {f:'Ne',att:'extra',axis:'N'}, {f:'Fi',att:'intro',axis:'F'}, {f:'Te',att:'extra',axis:'F'}, {f:'Si',att:'intro',axis:'N'} ],
  ENTP: [ {f:'Ne',att:'extra',axis:'N'}, {f:'Ti',att:'intro',axis:'F'}, {f:'Fe',att:'extra',axis:'F'}, {f:'Si',att:'intro',axis:'N'} ],
  ESTJ: [ {f:'Te',att:'extra',axis:'F'}, {f:'Si',att:'intro',axis:'N'}, {f:'Ne',att:'extra',axis:'N'}, {f:'Fi',att:'intro',axis:'F'} ],
  ESFJ: [ {f:'Fe',att:'extra',axis:'F'}, {f:'Si',att:'intro',axis:'N'}, {f:'Ne',att:'extra',axis:'N'}, {f:'Ti',att:'intro',axis:'F'} ],
  ENFJ: [ {f:'Fe',att:'extra',axis:'F'}, {f:'Ni',att:'intro',axis:'N'}, {f:'Se',att:'extra',axis:'N'}, {f:'Ti',att:'intro',axis:'F'} ],
  ENTJ: [ {f:'Te',att:'extra',axis:'F'}, {f:'Ni',att:'intro',axis:'N'}, {f:'Se',att:'extra',axis:'N'}, {f:'Fi',att:'intro',axis:'F'} ]
};

export function getStack(type){ return STACKS[type] || null; }

export const DEFAULT_ROLE_WEIGHTS = { dominant: 0.5, auxiliary: 0.3, tertiary: 0.15, inferior: 0.05 };

// Tetrahedral normals for an equivariant, highly symmetric arrangement
export const TETRAHEDRAL_NORMALS = [
  norm([ 1,  1,  1]),
  norm([ 1, -1, -1]),
  norm([-1,  1, -1]),
  norm([-1, -1,  1])
];

function norm(v){ const L = Math.hypot(v[0],v[1],v[2])||1; return [v[0]/L, v[1]/L, v[2]/L]; }
function cross(a,b){ return [a[1]*b[2]-a[2]*b[1], a[2]*b[0]-a[0]*b[2], a[0]*b[1]-a[1]*b[0]]; }
function dot(a,b){ return a[0]*b[0]+a[1]*b[1]+a[2]*b[2]; }
function add(a,b){ return [a[0]+b[0], a[1]+b[1], a[2]+b[2]]; }
function sub(a,b){ return [a[0]-b[0], a[1]-b[1], a[2]-b[2]]; }
function mul(a,s){ return [a[0]*s, a[1]*s, a[2]*s]; }

function tangentDirAt(p, n){
  // dir = normalize(n × p); if degenerate, jitter n slightly
  let dir = cross(n, p);
  const L = Math.hypot(dir[0],dir[1],dir[2]);
  if (L < 1e-6) {
    const nj = norm([n[0]+1e-3, n[1]-1e-3, n[2]+2e-3]);
    dir = cross(nj, p);
  }
  const L2 = Math.hypot(dir[0],dir[1],dir[2])||1; return [dir[0]/L2, dir[1]/L2, dir[2]/L2];
}

function rotateAroundAxis(v, axis, angle){
  // Rodrigues' rotation formula for unit axis
  const c = Math.cos(angle), s = Math.sin(angle);
  const a = axis; const vd = dot(a, v);
  const term1 = mul(v, c);
  const term2 = mul(cross(a, v), s);
  const term3 = mul(a, (1-c)*vd);
  const out = add(add(term1, term2), term3);
  const L = Math.hypot(out[0],out[1],out[2])||1; return [out[0]/L, out[1]/L, out[2]/L];
}

export function computeFunctionOffset(p, type, normals, weights=DEFAULT_ROLE_WEIGHTS, attitudeRollRad=0.1, offsetScale=1.0, maxDeg=12){
  // p, normals are arrays [x,y,z]
  const stack = getStack(type);
  if (!stack) return p.slice();
  const axes = { E: normals[0], N: normals[1], F: normals[2], P: normals[3] };
  const roleNames = ['dominant','auxiliary','tertiary','inferior'];
  let acc = [0,0,0];
  for (let i=0;i<stack.length;i++){
    const role = roleNames[i] || 'tertiary';
    const w = weights[role] || 0;
    const axisKey = stack[i].axis; // 'N' or 'F'
    const axis = axes[axisKey] || axes.N;
    const sign = (stack[i].f.endsWith('N')||stack[i].f.endsWith('F')) ? +1 : -1;
    let dir = tangentDirAt(p, axis);
    const roll = (stack[i].att === 'extra' ? +attitudeRollRad : -attitudeRollRad);
    dir = rotateAroundAxis(dir, p, roll);
    acc = add(acc, mul(dir, w*sign));
  }
  const accL = Math.hypot(acc[0],acc[1],acc[2]);
  if (accL < 1e-6) return p.slice();
  const dir = [acc[0]/accL, acc[1]/accL, acc[2]/accL];
  const delta = Math.min(offsetScale*accL, maxDeg*Math.PI/180);
  const axis = norm(cross(dir, p));
  const out = rotateAroundAxis(p, axis, delta);
  return out;
}
