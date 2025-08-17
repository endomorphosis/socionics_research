# Socionics Compass (Vite + Three.js)

## Development

1. Install dependencies (from the `compass` folder):
   ```bash
   npm install
   ```
2. Start the Vite dev server:
   ```bash
   npm run dev
   ```
   This will open the app at http://localhost:5173

## Build for Production

```bash
npm run build
```
Output will be in the `dist/` folder.

## Notes
- All scripts are now ES modules and imported via `main.js`.
- Uses npm package `three` instead of CDN.
- You can add more npm packages as needed.
