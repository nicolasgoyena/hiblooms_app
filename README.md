# HIBLOOMS — Detection and Visualization of Cyanobacterial Blooms in Reservoirs

Official repository of the **HIBLOOMS** project.

## Contents

* **Web application** (Streamlit): Explore, analyze, and download water quality spectral indices derived from **Sentinel‑2** imagery, with comparison to *in situ* measurements. The app includes a set of predefined Spanish reservoirs but also allows users to upload their own shapefiles to visualize and analyze any reservoir.
* **Auxiliary code** (modules and utilities): Helper scripts and functions used by the app.
* **CLI version** (`hiblooms_core.py`): Run the processing pipelines without a web interface.

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/<your-org>/hiblooms.git
   cd hiblooms
   ```
2. Create and activate a virtual environment (recommended):

   ```bash
   python -m venv venv
   source venv/bin/activate   # Linux/Mac
   venv\Scripts\activate      # Windows
   ```
3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Web Application (Streamlit)

Run the Streamlit app:

```bash
streamlit run app.py
```

This will start a local server where you can interact with the HIBLOOMS interface.

* **Predefined reservoirs**: Select from a list of Spanish reservoirs integrated into the system.
* **Custom reservoirs**: Upload your own shapefile to analyze and visualize the water quality of reservoirs outside Spain.

### CLI Version

Run the core processing logic directly:

```bash
python hiblooms_core.py --help
```

This version is designed for automated workflows and batch processing without a web interface.

## Repository Structure

```
├── .devcontainer/         # Development container configuration
├── images/                # Project images and assets
├── pages/                 # Streamlit multipage structure (e.g., login)
├── scripts/               # Helper scripts and workflows
├── shapefiles/            # Example shapefiles for reservoirs
├── app.py                 # Main Streamlit web application
├── hiblooms_core.py       # CLI version with core processing logic
├── requirements.txt       # Python dependencies
└── README.md              # Project documentation
```

## Citation

If you use HIBLOOMS in your work, please cite the project as:

```
HIBLOOMS Project — Detection and Visualization of Cyanobacterial Blooms in Reservoirs. University of Navarra, 2025. GitHub repository: https://github.com/<your-org>/hiblooms
```

## License

[MIT License](https://opensource.org/licenses/MIT)

---

> **Goal:** To provide knowledge and tools for the effective management of harmful algal blooms (HABs) in reservoirs, combining remote sensing, field data, and digital tools.
