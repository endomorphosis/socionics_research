// Dynamic legend for 4D projection and clustering
// Populates the #legend container with concise guidance.

(function initLegend(){
	if (typeof window === 'undefined') return;
	function byId(id){ return document.getElementById(id); }
	function swatch(color){ return `<span class="legend-color" style="background:${color}"></span>`; }

	const COLORS = {
		E: '#3399ff', I: '#ff7f33',
		S: '#33ff88', N: '#e633ff',
		T: '#ffd633', F: '#33ffd9',
		J: '#ff3399', P: '#33e0ff'
	};

	function fourDAxesHTML(){
		const showContours = !!(byId('chk-contours') && byId('chk-contours').checked);
		const preset = (function(){ const el = byId('sel-contour-preset'); return el && el.value ? el.value : 'vivid'; })();
		const intensity = (function(){ const el = byId('range-contour-intensity'); return el && el.value ? Number(el.value) : 0.55; })();
		const presetCols = (function(){
			if (preset === 'soft') return ['#f2cc8c','#f1a1b7','#b3e5f2','#c3e6c6'];
			if (preset === 'mono') return ['#ffffff','#e6e6e6','#cccccc','#b3b3b3'];
			return ['#FFC107','#E6194B','#00BCD4','#8BC34A'];
		})();
		return `
			<h2>4D Axes</h2>
			<div class="legend-row">${swatch(COLORS.E)} E vs ${swatch(COLORS.I)} I</div>
			<div class="legend-row">${swatch(COLORS.S)} S vs ${swatch(COLORS.N)} N</div>
			<div class="legend-row">${swatch(COLORS.T)} T vs ${swatch(COLORS.F)} F</div>
			<div class="legend-row">${swatch(COLORS.J)} J vs ${swatch(COLORS.P)} P</div>
			<div class="legend-row" style="margin-top:.4em;color:#555;font-size:.92em">
				Socionics types (ILE, LII, … or INTj/ENTp) are normalized to MBTI. For introverts, Socionics j/p flips to MBTI J/P.
			</div>
		<div class="legend-row" style="color:#555;font-size:.92em">Surface colors show MBTI regions on the sphere (nearest centroid).</div>
		${showContours ? `
			<h3 style="margin-top:.6em">Contour Guides</h3>
			<div class="legend-row">${swatch(presetCols[0])} E ↔ I</div>
			<div class="legend-row">${swatch(presetCols[1])} S ↔ N</div>
			<div class="legend-row">${swatch(presetCols[2])} T ↔ F</div>
			<div class="legend-row">${swatch(presetCols[3])} J ↔ P</div>
			<div class="legend-row" style="color:#555;font-size:.9em">Intensity: ${intensity.toFixed(2)}</div>
		` : ''}
		`;
	}

	function clustersHTML(){
		return `
			<h2>Clusters</h2>
			<div class="legend-row" style="color:#555;font-size:.95em">K-Means centroids are arranged via Fibonacci lattice; points jitter by similarity.</div>
		`;
	}

	function renderLegend(mode){
		const el = byId('legend');
		if (!el) return;
		el.innerHTML = mode === '4d' ? fourDAxesHTML() : clustersHTML();
	}

	function currentMode(){
		const sel = document.getElementById('sel-placement');
		return (sel && sel.value) || 'clusters';
	}

	function setup(){
		renderLegend(currentMode());
		const sel = document.getElementById('sel-placement');
		if (sel) sel.addEventListener('change', () => renderLegend(sel.value));
		// Also respond to late inserts of the selector
		const obs = new MutationObserver(() => {
			const s = document.getElementById('sel-placement');
			if (s && !s.__legendHooked) { s.addEventListener('change', () => renderLegend(s.value)); s.__legendHooked = true; renderLegend(s.value); }
			// Hook contour controls if they appear later
			const p = document.getElementById('sel-contour-preset');
			if (p && !p.__legendHooked) { p.addEventListener('change', () => renderLegend(currentMode())); p.__legendHooked = true; }
			const c = document.getElementById('chk-contours');
			if (c && !c.__legendHooked) { c.addEventListener('change', () => renderLegend(currentMode())); c.__legendHooked = true; }
			const ri = document.getElementById('range-contour-intensity');
			if (ri && !ri.__legendHooked) { ri.addEventListener('input', () => renderLegend(currentMode())); ri.__legendHooked = true; }
		});
		try { obs.observe(document.body, { childList: true, subtree: true }); } catch {}
	}

	if (document.readyState === 'loading') {
		document.addEventListener('DOMContentLoaded', setup, { once: true });
	} else {
		setup();
	}
})();
