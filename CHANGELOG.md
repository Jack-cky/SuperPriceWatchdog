# Change Log

## [2.0.0] PILOT Stage

### [2.1.3] - 2025-05-16
Modified pipeline to avoid problematic upstream data.
### Added
### Changed
- Changed API subdomain from 'api' to 'app' for a more reliable data source.
- Modified the pipeline only loads data if the latest data is available.
### Fixed

### [2.1.2] - 2025-05-04
Handled scenario when the data API return unexpectedly.
### Added
### Changed
- Reordered the functions in makefile.
### Fixed
- Deduplicate data versions when fetching data in batch.
- Pirce alert would only be sent out when the latest data is retrieved.

### [2.1.1] - 2025-04-18
Orchestrated pipeline and modularised webhook application.
### Added
- Added INI configuration file.
- Created log_omission SQL function to log down date without OPW records.
- Created omissions table in the database.
### Changed
- Consolidated webhook scripts into superpricewatchdog.
- Orchestrated the data pipeline into individual modular with Luigi framework.
- Removed configuration variables in environment.
### Fixed
- Alert send out time be around 12:00 every day.
- Keep ignored files when Github pulls.

### [2.0.3] - 2025-01-12
Enhanced script structure and product features.
### Added
- Dockerfiles for containerisation.
- Special offer message in price alert.
### Changed
- Aligned font of the price trend graph to NotoSansCJK-Bold in all environment.
- Modularised bot functions into classes for better maintainability.
- Updated the price alert logic from using the first quartile to normal standardisation because some items have a skewed price distribution.
### Fixed
- Date labels in the price trend are overlapping. Removed year labels from the y-axis of the plot.

### [2.0.2] - 2024-12-29
Enhanced project structure and error handling.
### Added
- Code of conduct for project collaboration.
- Error handling in the pipeline and bot.
- Example of .env for deployment template.
- Template index to render the README as the default index page.
### Changed
- Added the original price as output in the SQL function draw_deals.
- Enhanced code structure for maintainability and readability.
- Renamed the SQL function from get_random_deals to draw_deals.
- Updated the index page to include README content.
- Updated the Makefile to streamline deployment.
### Fixed
- Today's data is only available two days later, and the data-fetching logic adheres to this requirement.

## [2.0.1] 2024-12-23
Revamped data pipeline and Telegram bot.
### Added
- Hosted the service serverless on PythonAnywhere and Supabase.
- Implemented CI/CD integration between PythonAnywhere and GitHub.
### Changed
- Enhanced user experience in the Telegram chat.
- Replatformed the database from SQLite to PostgreSQL.
- Transitioned from polling to webhook for Telegram interactions.
- Switched from Pandas to Polars for data processing.
### Fixed


## [1.0.0] MVP Stage

### [1.0.1] - 2024-02-15
Initial repository.
