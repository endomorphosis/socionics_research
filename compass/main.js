// Entry point for Vite + Three.js app (Compass + Personality Planet)
import * as THREE from 'three';
import './style.css';
import './data.js';
import './legend.js';
import './add_personality.js';
// import './import_export.js';
// import './filter.js';
import './knn_client.js';
import './vectors_knn.js';
import './duckdb_loader.js';
import './search.js';
import './tooltip.js';
import './export_image.js';
import './kmeans.js';
import './globe.js';

// Choose UI view: 'planet' (default in dev) or 'compass'
const params = new URLSearchParams(window.location.search);
const viewParam = params.get('view');
const savedView = (() => { try { return localStorage.getItem('ui_view') || null; } catch { return null; } })();
const defaultView = (import.meta?.env?.DEV ? 'planet' : 'compass');
const view = (viewParam || savedView || defaultView);
try { localStorage.setItem('ui_view', view); } catch {}

if (view === 'compass') {
	// Load compass renderers only when requested
	import('./compass_clean.js').then(() => import('./compass.js')).catch((e) => console.warn('Compass load failed:', e));
} else {
	// Planet view uses globe.js + search.js; globe shown on demand (auto in dev handled in search.js)
}
// All logic is loaded via modules
