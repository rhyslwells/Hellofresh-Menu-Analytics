"""
SQLite DASHBOARD GENERATOR

Purpose
-------
Generates an interactive main page dashboard with exploratory data analysis
from all historical data. Creates `docs/index.html` with embedded Plotly charts.

Runs after gold layer is built and generates 4 interactive charts:
1. Ingredient trends over time (line chart)
2. Allergen patterns (temporal heatmap)
3. Recipe difficulty distribution (bar/histogram)
4. Weekly recipe difficulty trends (line chart)

Output
------
- Dashboard HTML: docs/index.html (with embedded interactive Plotly charts)

Usage
-----
From command line:
python scripts/5_dashboard.py

With GitHub Actions (after 3_gold_analytics.py):
python scripts/5_dashboard.py

Requirements
------------
- plotly
- pandas
"""

from pathlib import Path
from datetime import datetime, timezone
import sys

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from report_utils import (
    get_db_connection,
    build_html_page,
    wrap_chart_in_div,
    check_data_available,
    generate_ingredient_trends_chart,
    generate_allergen_patterns_chart,
    generate_recipe_difficulty_chart,
    generate_weekly_difficulty_chart,
    generate_summary_metrics_panel,
    get_summary_metrics,
    HAS_PLOTLY,
)


# ======================
# Configuration
# ======================

DB_PATH = Path("hfresh/hfresh.db")
PROJECT_ROOT = Path.cwd()
OUTPUT_FILE = PROJECT_ROOT / "docs" / "index.html"


# ======================
# Main Dashboard Generation
# ======================

def generate_dashboard() -> bool:
    """Generate the main dashboard HTML file."""
    print("\n📊 Generating HelloFresh Menu Analytics Dashboard\n")
    
    if not HAS_PLOTLY:
        print("❌ Plotly/pandas not available - cannot generate dashboard")
        return False
    
    try:
        conn = get_db_connection()
        
        # Check data availability
        print("Checking data availability...")
        data_check = check_data_available(conn)
        print(f"  - Weekly metrics: {data_check['weeks_in_metrics']} weeks")
        print(f"  - Ingredient trends: {data_check['ingredient_trends']} records")
        print(f"  - Allergen density: {data_check['allergen_density']} records")
        print(f"  - Active recipes: {data_check['active_recipes']} recipes\n")
        
        if data_check['weeks_in_metrics'] == 0:
            print("❌ No data available - run gold layer generation first")
            conn.close()
            return False
        
        # Generate all charts
        print("Generating charts...")
        ingredient_chart = generate_ingredient_trends_chart(conn)
        allergen_chart = generate_allergen_patterns_chart(conn)
        difficulty_chart = generate_recipe_difficulty_chart(conn)
        weekly_difficulty_chart = generate_weekly_difficulty_chart(conn)
        metrics_panel = generate_summary_metrics_panel(conn)
        
        # Build content sections
        content_sections = [
            {
                'title': 'Dashboard Overview',
                'content': '''
                <div class="intro-section">
                    <p>
                        Welcome to the HelloFresh Menu Analytics Dashboard. This interactive dashboard provides a comprehensive view of menu evolution, ingredient trends, and recipe patterns across all available historical data.
                    </p>
                    <p>
                        <strong>Last Updated:</strong> ''' + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") + '''
                    </p>
                </div>
                '''
            },
            {
                'title': 'Summary Metrics',
                'content': metrics_panel
            },
            {
                'title': '1. Ingredient Usage Trends',
                'content': wrap_chart_in_div(ingredient_chart, 'Top 8 Ingredients Tracked Over Time')
            },
            {
                'title': '2. Allergen Patterns',
                'content': wrap_chart_in_div(allergen_chart, 'Allergen Density Across All Weeks')
            },
            {
                'title': '3. Recipe Difficulty Distribution',
                'content': wrap_chart_in_div(difficulty_chart, 'How Many Recipes at Each Level')
            },
            {
                'title': '4. Weekly Recipe Difficulty Trends',
                'content': wrap_chart_in_div(weekly_difficulty_chart, 'Average Difficulty Per Week Over Time')
            },
            {
                'title': 'Weekly Reports Archive',
                'content': '''
                <div class="intro-section">
                    <p>
                        Detailed weekly snapshots of menu analysis are available in our reports archive. Each report includes week-specific tables, metrics, and trends.
                    </p>
                    <a href="weekly_reports/" class="btn">View Weekly Reports</a>
                </div>
                '''
            }
        ]
        
        # Build metadata
        metadata = {
            'Generated': datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            'Data Source': 'Gold Layer (refined from Silver Layer ingestion)',
            'Weeks Included': data_check['weeks_in_metrics'],
        }
        
        # Build HTML page
        html_content = build_html_page(
            title="HelloFresh Menu Analytics Dashboard",
            subtitle="Interactive analysis of menu evolution, ingredients, and recipes",
            content_sections=content_sections,
            metadata=metadata,
        )
        
        # Write to file
        print(f"\nWriting dashboard to {OUTPUT_FILE}...")
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_FILE.write_text(html_content, encoding='utf-8')
        
        conn.close()
        
        print(f"✅ Dashboard generated successfully!\n")
        print(f"📄 Output: {OUTPUT_FILE}")
        print(f"📊 Open in browser to view interactive charts")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generating dashboard: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = generate_dashboard()
    sys.exit(0 if success else 1)
