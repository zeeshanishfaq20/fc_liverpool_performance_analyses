# Football Clubs Performance Analysis

## Project Overview
This project provides a comprehensive analysis of football clubs' performances, with a special focus on Liverpool FC. Utilizing the Sportradar API, the project aims to extract, transform, and analyze match data to answer key business questions. The analysis includes match trends, player statistics, head-to-head comparisons, and upcoming match planning.

## Features & Key Insights
### Competitor Performance & Trends
- How has a competitor performed in past matches?
- Key statistics such as possession, shots, passes, and overall team efficiency.
- Performance variations across different seasons and competitions.
- Player contributions and their impact on team success.

### Match Insights & Predictions
- Identification of key match trends based on historical data.
- Performance analysis against specific opponents.
- Top-performing players across different statistical categories.

### Upcoming Match Scheduling & Planning
- Scheduling of upcoming matches for a given competitor.
- Expected lineups and squad availability for the next match.

## Technology Stack
This project leverages the following technologies:
- **dbt** – Data transformation and modeling.
- **Sportradar API** – Data extraction source.
- **Snowflake** – Cloud-based data storage.
- **Apache Airflow** – Workflow orchestration and scheduling (if applicable).
- **GitHub** – Version control and collaboration.

## Installation & Setup
To set up and run this project locally:
1. **Clone the repository:**  
   ```bash
   git clone https://github.com/zeeshanishfaq20/fc_liverpool_performance_analyses.git
   ```
2. **Set up the API key** – Obtain a Sportradar API key and configure it within the environment settings.
3. **Connect to Snowflake** – Ensure proper database configurations in dbt profiles.
4. **Install dependencies** – Run the following command:
   ```bash
   pip install -r requirements.txt
   ```
5. **Run dbt models** – Execute transformations using:
   ```bash
   dbt run
   ```

## Data Pipeline Workflow
### Data Extraction & Transformation
- **Initial Setup:** The dbt project is connected to Snowflake.
- **Data Extraction:** Data is extracted from the Sportradar API using Python scripts and loaded into Snowflake.
- **Staging Layer:**
  - A `staging` folder is created within the `models` directory.
  - A `source.yml` file is configured to define data sources.
  - Normalized staging tables are created for raw data processing.
- **Intermediate Layer:**
  - A folder named `intermediate` is used to denormalize data for simplified transformations.
- **Core Layer:**
  - Business logic is implemented for tables such as:
    - Overall club performance
    - Individual match performance
    - Player-specific statistics
    - Upcoming matches and expected lineups
    - Head-to-head comparisons

### Testing & Documentation
- **Tests:**
  - Generic and singular tests are implemented within the `staging` and `core` layers.
  - Source freshness tests and `dbt_expectations` are utilized for dataset validation.
- **Documentation:**
  - YAML files within `staging` and `core` ensure project documentation.
  - The `exposures` folder defines downstream usage, such as dashboard creation.

## Results & Insights
For a detailed view of the project structure, visit the GitHub repository:  
[Project Repository](https://github.com/zeeshanishfaq20/fc_liverpool_performance_analyses)

## Challenges & Learnings
### Challenges Faced
- Data cleaning complexities and ensuring the integrity of extracted data.
- Defining appropriate business logic to derive meaningful insights.

### Key Learnings & Improvements
- Gaining hands-on experience in setting up a complete dbt project independently.
- Future improvements include enhanced feature engineering and further automation.