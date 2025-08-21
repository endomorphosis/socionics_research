// MBTI data: types, dichotomy preferences, and simple function stacks

export const MBTI_TYPES = [
  'ISTJ','ISFJ','INFJ','INTJ',
  'ISTP','ISFP','INFP','INTP',
  'ESTP','ESFP','ENFP','ENTP',
  'ESTJ','ESFJ','ENFJ','ENTJ'
];

// True means E/N/F/P, False means I/S/T/J
export const TYPE_PREFS = {
  ISTJ: { E:false, N:false, F:false, P:false },
  ISFJ: { E:false, N:false, F:true,  P:false },
  INFJ: { E:false, N:true,  F:true,  P:false },
  INTJ: { E:false, N:true,  F:false, P:false },
  ISTP: { E:false, N:false, F:false, P:true  },
  ISFP: { E:false, N:false, F:true,  P:true  },
  INFP: { E:false, N:true,  F:true,  P:true  },
  INTP: { E:false, N:true,  F:false, P:true  },
  ESTP: { E:true,  N:false, F:false, P:true  },
  ESFP: { E:true,  N:false, F:true,  P:true  },
  ENFP: { E:true,  N:true,  F:true,  P:true  },
  ENTP: { E:true,  N:true,  F:false, P:true  },
  ESTJ: { E:true,  N:false, F:false, P:false },
  ESFJ: { E:true,  N:false, F:true,  P:false },
  ENFJ: { E:true,  N:true,  F:true,  P:false },
  ENTJ: { E:true,  N:true,  F:false, P:false }
};

// Minimal function stacks for tooltip context
export const FUNCTION_STACKS = {
  ISTJ: ['Si','Te','Fi','Ne'], ISFJ: ['Si','Fe','Ti','Ne'], INFJ: ['Ni','Fe','Ti','Se'], INTJ: ['Ni','Te','Fi','Se'],
  ISTP: ['Ti','Se','Ni','Fe'], ISFP: ['Fi','Se','Ni','Te'], INFP: ['Fi','Ne','Si','Te'], INTP: ['Ti','Ne','Si','Fe'],
  ESTP: ['Se','Ti','Fe','Ni'], ESFP: ['Se','Fi','Te','Ni'], ENFP: ['Ne','Fi','Te','Si'], ENTP: ['Ne','Ti','Fe','Si'],
  ESTJ: ['Te','Si','Ne','Fi'], ESFJ: ['Fe','Si','Ne','Ti'], ENFJ: ['Fe','Ni','Se','Ti'], ENTJ: ['Te','Ni','Se','Fi']
};

// Default great-circle normals (orthonormal-ish basis). You can tweak for aesthetics.
// Chosen so circles are visually distinct and not axis-aligned with the camera.
export const DEFAULT_NORMALS = {
  nE: normalize([1, 0.2, 0.1]),   // E vs I plane normal
  nN: normalize([0.1, 1, 0.2]),   // N vs S
  nF: normalize([0.2, 0.1, 1]),   // F vs T
  nP: normalize([-0.7, 0.6, 0.1]) // P vs J
};

export function normalize(v){
  const [x,y,z] = v; const n = Math.hypot(x,y,z) || 1; return [x/n, y/n, z/n];
}

export function regionKey(p, { nE, nN, nF, nP }){
  const sE = Math.sign(dot(nE, p)) >= 0 ? 1 : 0;
  const sN = Math.sign(dot(nN, p)) >= 0 ? 1 : 0;
  const sF = Math.sign(dot(nF, p)) >= 0 ? 1 : 0;
  const sP = Math.sign(dot(nP, p)) >= 0 ? 1 : 0;
  return (sE<<3) | (sN<<2) | (sF<<1) | sP;
}

export function dot(a,b){ return a[0]*b[0]+a[1]*b[1]+a[2]*b[2]; }
export function add(a,b){ return [a[0]+b[0], a[1]+b[1], a[2]+b[2]]; }
export function scale(a,s){ return [a[0]*s, a[1]*s, a[2]*s]; }
