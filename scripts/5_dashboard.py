"""
SQLite DASHBOARD GENERATOR

Purpose
-------
Generates an interactive main page dashboard with exploratory data analysis
from all historical data. Creates `docs/index.html` with embedded Plotly charts.

Runs after gold layer is built and generates charts including:
1. Ingredient trends over time (line chart)
2. Menu stability metrics (timeline)
3. Allergen patterns (temporal heatmap)
4. Recipe difficulty distribution (bar/histogram)
5. Summary metrics panel (aggregates)

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

import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import sys

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from report_utils import (
    get_db_connection,
    build_html_page,
    wrap_chart_in_div,
    get_all_historical_weeks,
    get_summary_metrics,
    check_data_available,
)

# Data visualization
try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    import pandas as pd
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False
    print("⚠️  Plotly or pandas not available - install with: pip install plotly pandas")


# ======================
# Configuration
# ======================

DB_PATH = Path("hfresh/hfresh.db")
PROJECT_ROOT = Path.cwd()
OUTPUT_FILE = PROJECT_ROOT / "docs" / "index.html"


# ======================
# Chart Generation Functions
# ======================

def generate_ingredient_trends_chart(conn: sqlite3.Connection) -> str:
    """
    Chart 1: Ingredient trends over time - line chart showing ingredient usage across all weeks.
    
    Returns
    -------
    str
        HTML of the chart div
    """
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating ingredient trends chart (all historical data)...")
    
    cursor = conn.cursor()
    
    # Get top 8 ingredients by average usage across all weeks
    cursor.execute("""
        SELECT ingredient_name
        FROM ingredient_trends
        GROUP BY ingredient_name
        HAVING AVG(recipe_count) > 0
        ORDER BY AVG(recipe_count) DESC
        LIMIT 8
    """)
    
    top_ingredients = [row[0] for row in cursor.fetchall()]
    
    if not top_ingredients:
        print("    ⚠️  No ingredient trends data available")
        return ""
    
    # Get weekly data for top ingredients
    placeholders = ", ".join(["?" for _ in top_ingredients])
    cursor.execute(f"""
        SELECT 
            week_start_date,
            ingredient_name,
            recipe_count
        FROM ingredient_trends
        WHERE ingredient_name IN ({placeholders})
        ORDER BY week_start_date ASC, ingredient_name
    """, top_ingredients)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No weekly ingredient data available")
        return ""
    
    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=['week_start_date', 'ingredient_name', 'recipe_count'])
    
    # Create line chart
    fig = px.line(
        df,
        x='week_start_date',
        y='recipe_count',
        color='ingredient_name',
        title='Ingredient Usage Trends Over Time (Top 8)',
        labels={'week_start_date': 'Week', 'recipe_count': 'Recipe Count', 'ingredient_name': 'Ingredient'},
        height=450
    )
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='#f9f9f9',
        xaxis_showgrid=True,
        yaxis_showgrid=True,
        hovermode='x unified',
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
    )
    
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    print(f"    ✓ Generated with {len(df)} data points from {df['week_start_date'].nunique()} weeks")
    
    return fig.to_html(include_plotlyjs=False, div_id="ingredient_trends_chart")


def generate_menu_stability_chart(conn: sqlite3.Connection) -> str:
    """
    Chart 2: Menu stability metrics - timeline chart showing overlap % and churn over time.
    
    Returns
    -------
    str
        HTML of the chart div
    """
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating menu stability metrics chart...")
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            week_start_date,
            overlap_with_prev_week * 100 as overlap_pct,
            (CASE WHEN overlap_with_prev_week IS NULL THEN NULL 
                  ELSE (1 - overlap_with_prev_week) * 100 END) as churn_pct,
            recipes_added,
            recipes_removed
        FROM menu_stability_metrics
        WHERE week_start_date IS NOT NULL
        ORDER BY week_start_date ASC
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No menu stability data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'overlap_pct', 'churn_pct', 'recipes_added', 'recipes_removed'])
    
    # Create subplots with 2 y-axes
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add overlap percentage (left axis)
    fig.add_trace(
        go.Scatter(
            x=df['week_start_date'],
            y=df['overlap_pct'],
            name='Menu Overlap %',
            mode='lines+markers',
            line=dict(color='#2E86AB', width=2),
            marker=dict(size=6),
        ),
        secondary_y=False,
    )
    
    # Add recipes added/removed (right axis)
    fig.add_trace(
        go.Bar(
            x=df['week_start_date'],
            y=df['recipes_added'],
            name='Recipes Added',
            marker=dict(color='#28A745'),
            opacity=0.6,
        ),
        secondary_y=True,
    )
    
    fig.add_trace(
        go.Bar(
            x=df['week_start_date'],
            y=df['recipes_removed'],
            name='Recipes Removed',
            marker=dict(color='#DC3545'),
            opacity=0.6,
        ),
        secondary_y=True,
    )
    
    # Update layout
    fig.update_layout(
        title='Menu Stability Metrics Over Time',
        height=450,
        plot_bgcolor='white',
        paper_bgcolor='#f9f9f9',
        hovermode='x unified',
        xaxis_title='Week',
        barmode='group',
    )
    
    fig.update_yaxes(title_text="Overlap %", secondary_y=False, showgrid=True, gridcolor='lightgray')
    fig.update_yaxes(title_text="Recipes Added/Removed", secondary_y=True)
    fig.update_xaxes(showgrid=False)
    
    print(f"    ✓ Generated stability metrics for {len(df)} weeks")
    
    return fig.to_html(include_plotlyjs=False, div_id="menu_stability_chart")


def generate_allergen_patterns_chart(conn: sqlite3.Connection) -> str:
    """
    Chart 3: Allergen patterns - temporal heatmap of allergen density across all weeks.
    
    Returns
    -------
    str
        HTML of the chart div
    """
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating allergen patterns heatmap...")
    
    cursor = conn.cursor()
    
    # Get allergen data aggregated by week
    cursor.execute("""
        SELECT 
            week_start_date,
            allergen_name,
            ROUND(percentage_of_menu, 2) as allergen_density
        FROM allergen_density
        ORDER BY week_start_date ASC, allergen_name
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No allergen density data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'allergen_name', 'allergen_density'])
    
    # Pivot for heatmap
    pivot_df = df.pivot(index='allergen_name', columns='week_start_date', values='allergen_density')
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale='YlOrRd',
        colorbar=dict(title="Density"),
        hovertemplate='<b>%{y}</b><br>Week: %{x}<br>Density: %{z:.3f}<extra></extra>',
    ))
    
    fig.update_layout(
        title='Allergen Density Patterns Over Time',
        xaxis_title='Week',
        yaxis_title='Allergen Type',
        height=400,
        plot_bgcolor='white',
        paper_bgcolor='#f9f9f9',
    )
    
    print(f"    ✓ Generated heatmap for {pivot_df.shape[0]} allergens across {pivot_df.shape[1]} weeks")
    
    return fig.to_html(include_plotlyjs=False, div_id="allergen_patterns_chart")


def generate_recipe_difficulty_chart(conn: sqlite3.Connection) -> str:
    """
    Chart 4: Recipe difficulty distribution - histogram/bar chart of recipes by difficulty level.
    
    Returns
    -------
    str
        HTML of the chart div
    """
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating recipe difficulty distribution chart...")
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            difficulty,
            COUNT(*) as recipe_count
        FROM recipes
        WHERE is_active = 1
        GROUP BY difficulty
        ORDER BY difficulty ASC
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No recipe difficulty data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['difficulty', 'recipe_count'])
    
    # Create bar chart
    fig = px.bar(
        df,
        x='difficulty',
        y='recipe_count',
        title='Recipe Distribution by Difficulty Level',
        labels={'difficulty': 'Difficulty Level', 'recipe_count': 'Number of Recipes'},
        height=400,
        color='difficulty',
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='#f9f9f9',
        showlegend=False,
        hovermode='x unified',
    )
    
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor='lightgray')
    
    print(f"    ✓ Generated distribution for {df['recipe_count'].sum()} total recipes")
    
    return fig.to_html(include_plotlyjs=False, div_id="difficulty_chart")


def generate_menu_evolution_chart(conn: sqlite3.Connection) -> str:
    """
    Chart 5: Menu evolution - area chart showing total recipes and new recipes per week.
    
    Returns
    -------
    str
        HTML of the chart div
    """
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating menu evolution chart...")
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            week_start_date,
            total_recipes,
            new_recipes,
            returning_recipes
        FROM weekly_menu_metrics
        ORDER BY week_start_date ASC
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No menu evolution data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'total_recipes', 'new_recipes', 'returning_recipes'])
    
    # Create stacked area chart
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['week_start_date'],
        y=df['returning_recipes'],
        name='Returning Recipes',
        mode='lines',
        line=dict(color='#2E86AB'),
        stackgroup='one',
        fillcolor='#2E86AB',
    ))
    
    fig.add_trace(go.Scatter(
        x=df['week_start_date'],
        y=df['new_recipes'],
        name='New Recipes',
        mode='lines',
        line=dict(color='#A23B72'),
        stackgroup='one',
        fillcolor='#A23B72',
    ))
    
    fig.update_layout(
        title='Menu Evolution: New vs Returning Recipes',
        xaxis_title='Week',
        yaxis_title='Number of Recipes',
        height=400,
        plot_bgcolor='white',
        paper_bgcolor='#f9f9f9',
        hovermode='x unified',
    )
    
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor='lightgray')
    
    print(f"    ✓ Generated menu evolution for {len(df)} weeks")
    
    return fig.to_html(include_plotlyjs=False, div_id="menu_evolution_chart")


def generate_summary_metrics_panel(conn: sqlite3.Connection) -> str:
    """
    Generate summary metrics cards showing key aggregates.
    
    Returns
    -------
    str
        HTML of the metrics panel
    """
    print("  → Generating summary metrics panel...")
    
    metrics = get_summary_metrics(conn)
    
    html_parts = []
    html_parts.append('<div class="metrics-grid">')
    
    # Metric cards
    cards = [
        ('total_recipes', 'Total Recipes', 'recipes'),
        ('total_ingredients', 'Unique Ingredients', 'ingredients'),
        ('total_weeks', 'Weeks of Data', 'weeks'),
        ('avg_recipes_per_week', 'Avg Recipes/Week', 'recipes'),
    ]
    
    for key, label, unit in cards:
        value = metrics.get(key, 0)
        html_parts.append(f'''
    <div class="metric-card">
        <div class="label">{label}</div>
        <div class="value">{value}</div>
        <div class="label">{unit}</div>
    </div>
        ''')
    
    html_parts.append('</div>')
    
    print("    ✓ Generated 4 metric cards")
    
    return "\n".join(html_parts)


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
        stability_chart = generate_menu_stability_chart(conn)
        allergen_chart = generate_allergen_patterns_chart(conn)
        difficulty_chart = generate_recipe_difficulty_chart(conn)
        evolution_chart = generate_menu_evolution_chart(conn)
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
                'title': '2. Menu Stability Metrics',
                'content': wrap_chart_in_div(stability_chart, 'Menu Overlap and Recipe Changes')
            },
            {
                'title': '3. Allergen Patterns',
                'content': wrap_chart_in_div(allergen_chart, 'Allergen Density Across All Weeks')
            },
            {
                'title': '4. Recipe Difficulty Distribution',
                'content': wrap_chart_in_div(difficulty_chart, 'How Many Recipes at Each Level')
            },
            {
                'title': '5. Menu Evolution',
                'content': wrap_chart_in_div(evolution_chart, 'New vs Returning Recipes Over Time')
            },
            {
                'title': 'Data Quality Notes',
                'content': '''
                <div class="data-quality">
                    <ul>
                        <li>Dashboard generated from Gold layer analytics tables</li>
                        <li>All charts include all available historical data</li>
                        <li>Dates are based on menu week start dates (typically Monday)</li>
                        <li>Interactive charts - hover for details, click legend items to toggle series</li>
                    </ul>
                </div>
                '''
            }
        ]
        
        # Build metadata
        metadata = {
            'Generated': datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            'Data Source': 'Gold Layer Analytics Tables',
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
