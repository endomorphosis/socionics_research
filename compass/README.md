# Socionics Compass (Vite + Three.js)

A 3D interactive compass for visualizing personality data in socionics research.

## Features

- **3D Visualization**: Interactive Three.js compass with personality markers
- **RAG Search**: Search and add personalities from the research database
- **Manual Entry**: Add custom personalities with coordinates and metadata
- **Filtering**: Filter personalities by quadrant, type, and color proximity
- **Export**: Export compass as PNG image and personality data as JSON

## Development

1. Install dependencies (from the `compass` folder):
   ```bash
   npm install
   ```

2. Start the development server (Vite only, no search):
   ```bash
   npm run dev
   ```
   This will open the app at http://localhost:5173

## Production with RAG Search

To use the full application with working RAG search functionality:

1. Build the application:
   ```bash
   npm run build
   ```

2. Start the Express server:
   ```bash
   node server.js
   ```
   
3. Access the application at http://localhost:3000

The Express server provides the `/api/search` endpoint that enables the RAG search functionality.

## Usage

### RAG Search
- Type personality names (e.g., "Einstein", "Tesla", "Gandhi") in the search box
- Click on search results to add personalities to the 3D compass
- Search works with names, types, socionics codes, and descriptions

### Manual Entry
- Use the "Add Personality" form to manually enter coordinates and metadata
- Coordinates range from -1 to 1 for X, Y, Z axes
- Choose colors to represent different quadrants or categories

### Filtering
- Use checkboxes to filter by quadrant colors and personality types
- Adjust color proximity filter using the color wheel and tolerance slider

## Notes
- All scripts are now ES modules and imported via `main.js`.
- Uses npm package `three` instead of CDN.
- RAG search includes fallback sample data when parquet files are unavailable.
- You can add more npm packages as needed.
