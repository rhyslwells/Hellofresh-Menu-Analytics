## Building an Automated HelloFresh Menu Analytics Pipeline

### What the project does

This [project](https://github.com/rhyslwells/Hellofresh-Menu-Analytics/tree/main) builds a small but complete analytics workflow around HelloFresh menus. Each week, menu and recipe data are pulled from the HelloFresh API and stored in a SQLite database. From there, a set of SQL analytics queries are run to examine recent menu changes and how they compare with the previous week.

The outputs of these queries are used to generate an HTML report. The report highlights notable features in the latest data, such as changes in menu composition and recurring patterns, and pairs query results with simple graphs to make week-to-week differences easier to interpret. The report is produced automatically and published to [GitHub Pages](https://rhyslwells.github.io/Hellofresh-Menu-Analytics/) on a regular basis.

The goal was to keep the workflow reproducible and easy to run. I initially explored using Databricks as an execution environment, but when working within the limits of the free tier, API access and orchestration become more constrained. For this project, GitHub Actions provided a simpler and more transparent way to run the pipeline and publish results.

### What I explored while building it

Although the project itself is modest in scope, it became a useful space to explore tooling and structure:

* **ETL design with a Medallion structure:** Data is organised into Bronze and Silver layers, separating raw API ingestion from cleaned, query-ready tables.
* **Automation with GitHub Actions:** Data ingestion, analytics, and report publishing are fully automated.
* **Secure configuration:** The HelloFresh API key is managed using GitHub Secrets and injected at runtime.
* **Repository governance:** GitHub rulesets are used to protect the `main` branch and control merges.
* **Copilot-assisted development:** GitHub Copilot in VS Code is used to translate GitHub issues into concrete changes in the repository.
* **Documentation as structure:** A blueprint document in the `docs` folder describes the repository layout and overall workflow, alongside documentation cataloguing significant changes.
* **Schema memory aid:** A Mermaid diagram captures the SQLite database schema and relationships for reference during analysis.

Overall, the project focuses on building a clear, automated analytics workflow, while also serving as a sandbox for exploring modern development, documentation, and governance practices in a small data engineering codebase.