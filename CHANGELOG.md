# Changelog

## [2.3.2] - 2026-08-22

Improved pipeline deployment and handled omitted OPW data versions.

### Added
- Added a script to configure the production GitHub environment and secrets.
- Added a Makefile target to pull the pipeline image from Docker Hub.

### Changed
- Updated the scheduled workflow to use the production environment and retry pipeline image pulls.
- Updated the scheduled workflow to use the configured bot forwarding URL for health checks and alerts.
- Updated the pipeline to track omitted OPW versions separately from available data versions.
- Streamlined version extraction across the scope, download, and database tasks.
- Updated the bot feedback link to use GitHub Discussions.
- Configured a five-minute statement timeout for service-role API requests.

### Fixed
- Continued loading available records when the expected OPW version is omitted.

## [2.3.1] - 2026-06-30

Refactored project structure and modularised the bot and pipeline components.

### Added
- Added modular structure for `bot` and `pipeline` processes within a unified `spwd` package.
- Created `spwd/shared/` directory to share configuration and clients.
- Added modularised database scripts `functions.sql`, `init.sql`, `schema.sql`.
- Added GitHub Actions workflow `.github/workflows/pipeline.yml` to schedule and run the pipeline.
- Added Docker Compose configuration `spwd/compose.yaml` for building and managing container services.

### Changed
- Refactored `superpricewatchdog` package logic into the `spwd` module.
- Consolidated Dockerfiles into separate `bot` and `pipeline` configurations inside `spwd/`.
- Updated Python packaging configuration in `pyproject.toml` with optional dependency targets `bot` and `pipeline`, and updated the package name to `spwd`.
- Updated `makefile` replacing local setup with Docker Compose deployment commands.
- Re-architected project repository structure.
- Updated `.gitignore` and `.dockerignore` to streamline ignored build, cache, and IDE files.
- Refined documentation text and formatting in `README.md` and `KNOWN_ISSUES.md`.

### Fixed
- Updated `database/functions.sql` so `update_deals` computes summary statistics from each SKU's daily minimum price, making `q0_price`, average price, and standard deviation date-aware.

### Removed
- Removed monolithic `superpricewatchdog` package and modules.
- Removed `setup.py` and `requirements.txt` in favour of `pyproject.toml` dependency management.
- Removed deprecated database schema files and testing scripts.
- Removed `CODE_OF_CONDUCT.md`.
- Removed `docker/deploy.sh` script and the root `Dockerfile`.

## [2.2.1] - 2025-06-22

Integrated the pipeline into the web application.

### Added
- Added a `.dockerignore` file.
- Added a `robots.txt` file to prevent web crawling.
- Added custom error pages for `404` and `500` errors.
- Created a web route for the pipeline.

### Changed
- Changed the API subdomain back to `app` due to unsupported configurations on PythonAnywhere.
- Configured the web application to run using Gunicorn.
- Consolidated multiple `Dockerfile`s into a single file.
- Packaged `superpricewatchdog` as a library.
- Separated CSS from the `index.html` file.
- Updated environment variable names for consistency.
- Updated solution design images.

### Fixed
- Fixed pipeline to calling internal functions instead of posting to the API because PythonAnywhere has only 1 worker for free tier.

## [2.1.3] - 2025-05-16

Modified pipeline to avoid problematic upstream data.

### Changed
- Changed API subdomain from `api` to `app` for a more reliable data source.
- Modified the pipeline only loads data if the latest data is available.

## [2.1.2] - 2025-05-04

Handled scenario when the data API returns unexpectedly.

### Changed
- Reordered the functions in `makefile`.

### Fixed
- Deduplicate data versions when fetching data in batch.
- Price alert would only be sent out when the latest data is retrieved.

## [2.1.1] - 2025-04-18

Orchestrated pipeline and modularised webhook application.

### Added
- Added `.ini` configuration file.
- Created `log_omission` SQL function to log down date without OPW records.
- Created `omissions` table in the database.

### Changed
- Consolidated webhook scripts into `superpricewatchdog`.
- Orchestrated the data pipeline into individual modular with Luigi framework.
- Removed configuration variables in environment.

### Fixed
- Alert send out time be around 12:00 every day.
- Keep ignored files when Github pulls.

## [2.0.3] - 2025-01-12

Enhanced script structure and product features.

### Added
- `Dockerfile` for containerisation.
- Special offer message in price alert.

### Changed
- Aligned font of the price trend graph to `NotoSansCJK-Bold` in all environment.
- Modularised bot functions into classes for better maintainability.
- Updated the price alert logic from using the first quartile to normal standardisation because some items have a skewed price distribution.

### Fixed
- Date labels in the price trend are overlapping. Removed year labels from the y-axis of the plot.

## [2.0.2] - 2024-12-29

Enhanced project structure and error handling.

### Added
- Code of conduct for project collaboration.
- Error handling in the pipeline and bot.
- Example of `.env` for deployment template.
- Template index to render the `README.md` as the default index page.

### Changed
- Added the original price as output in the SQL function `draw_deals`.
- Enhanced code structure for maintainability and readability.
- Renamed the SQL function from `get_random_deals` to `draw_deals`.
- Updated the index page to include `README.md` content.
- Updated the `makefile` to streamline deployment.

### Fixed
- Today's data is only available two days later, and the data-fetching logic adheres to this requirement.

## [2.0.1] - 2024-12-23

Revamped data pipeline and Telegram bot.

### Added
- Hosted the service serverless on PythonAnywhere and Supabase.
- Implemented CI/CD integration between PythonAnywhere and GitHub.

### Changed
- Enhanced user experience in the Telegram chat.
- Replatformed the database from SQLite to PostgreSQL.
- Transferred from polling to webhook for Telegram interactions.
- Switched from Pandas to Polars for data processing.

## [1.0.1] - 2024-02-15

Initial repository.
