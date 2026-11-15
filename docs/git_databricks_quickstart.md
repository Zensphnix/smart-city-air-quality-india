# Simple Databricks setup using Git

Git synchronizes the project code (notebooks, scripts, documentation, and configuration). Raw API files still go to the Unity Catalog Volume, but the normal first run creates them directly from the extraction notebook.

## Part A — Put the local project on GitHub

1. Sign in to GitHub and select **New repository**.
2. Name it `smart-city-air-quality-india`. Choose **Private** or **Public**. Do **not** add a README, `.gitignore`, or license because this project already includes them.
3. Copy the repository HTTPS address, for example `https://github.com/YOUR-USERNAME/smart-city-air-quality-india.git`.
4. Open PowerShell and run the following. Replace only `YOUR-USERNAME` in the final command.

```powershell
cd "C:\Users\user1.SHASHIRAY.000\Documents\Codex\2026-11-15\smart-city-air-quality-india"
git init
git add README.md .gitignore requirements.txt config notebooks src dashboard docs
git commit -m "Initial Smart City AQI pipeline"
git branch -M main
git remote add origin https://github.com/YOUR-USERNAME/smart-city-air-quality-india.git
git push -u origin main
```

If `git` is not recognized, install Git for Windows, reopen PowerShell, and repeat the commands. Do not add `.venv` or `data`; `.gitignore` excludes both.

## Part B — Clone it into Databricks

1. In Databricks, select **Workspace**.
2. Select **Create > Git folder**.
3. Choose Git provider **GitHub** and paste your repository HTTPS address.
4. Set branch to `main`, choose a folder name such as `smart-city-aqi`, then select **Create**.
5. Open that Git folder. The `notebooks` folder contains the numbered Databricks notebooks.

## Part C — Run in the simple order

1. Open `notebooks/00_setup.py` and select **Run all**. This creates the catalog, schemas, and raw Volume.
2. Open `notebooks/01_extract.py`, leave the default dates, and select **Run all**. It now creates the city reference file and writes raw API data itself.
3. Run `02_bronze.py`, `03_silver.py`, `04_gold.py`, and `05_validation.py` in that order.
4. If the extraction notebook reports blocked internet access, run the local extraction command, then upload the resulting JSON files to the matching folders in the Volume.

## Updating Git after a local change

```powershell
git add .
git commit -m "Describe the change"
git push
```

In Databricks, open the Git folder and select **Pull** / **Update** to receive the latest changes.
