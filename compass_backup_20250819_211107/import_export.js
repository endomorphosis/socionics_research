// Import/Export logic for personalities
window.addEventListener('DOMContentLoaded', () => {
  // Export button
  const exportBtn = document.createElement('button');
  exportBtn.id = 'export-personalities';
  exportBtn.textContent = 'Export Personalities';
  exportBtn.type = 'button';
  exportBtn.style.marginRight = '1em';

  // Import button and file input
  const importBtn = document.createElement('button');
  importBtn.id = 'import-personalities';
  importBtn.textContent = 'Import Personalities';
  importBtn.type = 'button';
  const clearBtn = document.createElement('button');
  clearBtn.id = 'clear-personalities';
  clearBtn.textContent = 'Clear All';
  clearBtn.type = 'button';
  const fileInput = document.createElement('input');
  fileInput.type = 'file';
  fileInput.accept = '.json,application/json';
  fileInput.style.display = 'none';

  // Button container
  const btnContainer = document.createElement('div');
  btnContainer.id = 'import-export-controls';
  btnContainer.appendChild(exportBtn);
  btnContainer.appendChild(importBtn);
  btnContainer.appendChild(clearBtn);
  btnContainer.appendChild(fileInput);
  document.body.appendChild(btnContainer);

  // Persist across reloads
  try {
    const saved = localStorage.getItem('compass_personalityData');
    if (saved) {
      const arr = JSON.parse(saved);
      if (Array.isArray(arr)) {
        window.personalityData = arr;
      }
    }
  } catch {}

  // Export handler
  exportBtn.addEventListener('click', () => {
    const data = window.personalityData || [];
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'personalities.json';
    document.body.appendChild(a);
    a.click();
    setTimeout(() => {
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }, 100);
  });

  // Import handler
  importBtn.addEventListener('click', () => fileInput.click());
  fileInput.addEventListener('change', (e) => {
    const file = fileInput.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = function(evt) {
      try {
        const arr = JSON.parse(evt.target.result);
        if (Array.isArray(arr)) {
          window.personalityData = arr;
          if (window.refreshCompass) window.refreshCompass();
          try { localStorage.setItem('compass_personalityData', JSON.stringify(arr)); } catch {}
        } else {
          alert('Invalid file format.');
        }
      } catch (err) {
        alert('Could not parse file.');
      }
    };
    reader.readAsText(file);
    fileInput.value = '';
  });

  // Clear all handler
  clearBtn.addEventListener('click', () => {
    window.personalityData = [];
    try { localStorage.setItem('compass_personalityData', JSON.stringify([])); } catch {}
    if (window.refreshCompass) window.refreshCompass();
  });

  // Auto-save on changes (simple polling)
  setInterval(() => {
    try {
      const data = window.personalityData || [];
      localStorage.setItem('compass_personalityData', JSON.stringify(data));
    } catch {}
  }, 1500);
});
