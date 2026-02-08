"""
Dashboard-Specific Utilities

Purpose
-------
Chart generation and visualization functions specific to the main dashboard
(5_dashboard.py). These functions create historical data analytics charts
across all available data.

Functions
---------
Chart Generation (Historical Data)
  - generate_ingredient_trends_chart(): Top 8 ingredients over time
  - generate_allergen_patterns_chart(): Allergen density heatmap
  - generate_recipe_difficulty_chart(): Recipe distribution by difficulty
  - generate_menu_evolution_chart(): Area chart of menu changes
  - generate_weekly_difficulty_chart(): Avg difficulty per week
  - generate_recipe_tags_chart(): Tag distribution over time
  - generate_ingredient_complexity_chart(): Ingredient count trends
  - generate_summary_metrics_panel(): Summary metric cards
"""

import sqlite3
from pathlib import Path
from typing import Dict, Any

# Data visualization
try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    import pandas as pd
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

from report_utils import (
    extract_plotly_chart,
    _get_standard_layout,
    _apply_grid_styling,
    COLOR_PRIMARY,
    COLOR_ACCENT,
    COLOR_SUCCESS,
    COLOR_DANGER,
    get_summary_metrics,
)


# ======================
# Dashboard Chart Generation
# ======================

def generate_ingredient_trends_chart(conn: sqlite3.Connection) -> str:
    """Chart 1: Ingredient trends over time - line chart showing ingredient usage across all weeks.
    
    Creates a line chart of the top 8 ingredients tracked across all historical weeks.
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
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'ingredient_name', 'recipe_count'])
    
    fig = px.line(
        df,
        x='week_start_date',
        y='recipe_count',
        color='ingredient_name',
        title='Ingredient Usage Trends Over Time (Top 8)',
        labels={'week_start_date': 'Week', 'recipe_count': 'Recipe Count', 'ingredient_name': 'Ingredient'},
        height=450
    )
    
    fig.update_layout(_get_standard_layout(
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
    ))
    _apply_grid_styling(fig, x_grid=True, y_grid=True)
    
    print(f"    ✓ Generated with {len(df)} data points from {df['week_start_date'].nunique()} weeks")
    
    chart_html = fig.to_html(include_plotlyjs=False, div_id="ingredient_trends_chart")
    return extract_plotly_chart(chart_html)


def generate_allergen_patterns_chart(conn: sqlite3.Connection) -> str:
    """Chart 3: Allergen patterns - temporal heatmap of allergen density across all weeks.
    
    Creates a heatmap showing how allergen density changes over time.
    """
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating allergen patterns heatmap...")
    
    cursor = conn.cursor()
    
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
    
    pivot_df = df.pivot(index='allergen_name', columns='week_start_date', values='allergen_density')
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale='YlOrRd',
        colorbar=dict(title="Density"),
        hovertemplate='<b>%{y}</b><br>Week: %{x}<br>Density: %{z:.3f}<extra></extra>',
    ))
    
    fig.update_layout(_get_standard_layout(
        title='Allergen Density Patterns Over Time',
        xaxis_title='Week',
        yaxis_title='Allergen Type',
        height=400,
    ))
    
    print(f"    ✓ Generated heatmap for {pivot_df.shape[0]} allergens across {pivot_df.shape[1]} weeks")
    
    chart_html = fig.to_html(include_plotlyjs=False, div_id="allergen_patterns_chart")
    return extract_plotly_chart(chart_html)


def generate_recipe_difficulty_chart(conn: sqlite3.Connection) -> str:
    """Chart 4: Recipe difficulty distribution - bar chart of recipes by difficulty level.
    
    Shows the distribution of all recipes across different difficulty levels.
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
    
    fig.update_layout(_get_standard_layout(showlegend=False))
    _apply_grid_styling(fig, x_grid=False, y_grid=True)
    
    print(f"    ✓ Generated distribution for {df['recipe_count'].sum()} total recipes")
    
    chart_html = fig.to_html(include_plotlyjs=False, div_id="difficulty_chart")
    return extract_plotly_chart(chart_html)


def generate_menu_evolution_chart(conn: sqlite3.Connection) -> str:
    """Chart 5: Menu evolution - area chart showing total recipes and new recipes per week.
    
    Shows how the menu composition changes week-to-week.
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
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['week_start_date'],
        y=df['returning_recipes'],
        name='Returning Recipes',
        mode='lines',
        line=dict(color=COLOR_PRIMARY),
        stackgroup='one',
        fillcolor=COLOR_PRIMARY,
    ))
    
    fig.add_trace(go.Scatter(
        x=df['week_start_date'],
        y=df['new_recipes'],
        name='New Recipes',
        mode='lines',
        line=dict(color=COLOR_ACCENT),
        stackgroup='one',
        fillcolor=COLOR_ACCENT,
    ))
    
    fig.update_layout(_get_standard_layout(
        title='Menu Evolution: New vs Returning Recipes',
        xaxis_title='Week',
        yaxis_title='Number of Recipes',
        height=400,
    ))
    _apply_grid_styling(fig, x_grid=False, y_grid=True)
    
    print(f"    ✓ Generated menu evolution for {len(df)} weeks")
    
    chart_html = fig.to_html(include_plotlyjs=False, div_id="menu_evolution_chart")
    return extract_plotly_chart(chart_html)


def generate_weekly_difficulty_chart(conn: sqlite3.Connection) -> str:
    """Chart: Weekly average recipe difficulty - line chart showing difficulty trends per week.
    
    Tracks how the average difficulty of recipes changes over time.
    """
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating weekly recipe difficulty chart...")
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            week_start_date,
            avg_difficulty
        FROM weekly_menu_metrics
        WHERE avg_difficulty IS NOT NULL
        ORDER BY week_start_date ASC
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No weekly difficulty data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'avg_difficulty'])
    
    fig = px.line(
        df,
        x='week_start_date',
        y='avg_difficulty',
        title='Average Recipe Difficulty Per Week',
        labels={'week_start_date': 'Week', 'avg_difficulty': 'Average Difficulty'},
        height=450,
        markers=True
    )
    
    fig.update_traces(
        line=dict(color=COLOR_ACCENT, width=3),
        marker=dict(size=8)
    )
    
    fig.update_layout(_get_standard_layout(
        showlegend=False,
        yaxis=dict(range=[0, 5]),
    ))
    _apply_grid_styling(fig, x_grid=True, y_grid=True)
    
    print(f"    ✓ Generated weekly difficulty chart for {len(df)} weeks")
    
    chart_html = fig.to_html(include_plotlyjs=False, div_id="weekly_difficulty_chart")
    return extract_plotly_chart(chart_html)


def generate_recipe_tags_chart(conn: sqlite3.Connection) -> str:
    """Chart: Recipe tags analytics - stacked area chart showing tag distribution per week.
    
    Shows how the top recipe tags change over time.
    """
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating recipe tags analytics chart...")
    
    cursor = conn.cursor()
    
    # Get top 8 tags by average usage
    cursor.execute("""
        SELECT tag_name
        FROM recipe_tags_analytics
        GROUP BY tag_name
        HAVING AVG(recipe_count) > 0
        ORDER BY AVG(recipe_count) DESC
        LIMIT 8
    """)
    
    top_tags = [row[0] for row in cursor.fetchall()]
    
    if not top_tags:
        print("    ⚠️  No recipe tags data available")
        return ""
    
    # Get weekly data for top tags
    placeholders = ", ".join(["?" for _ in top_tags])
    cursor.execute(f"""
        SELECT 
            week_start_date,
            tag_name,
            percentage_of_menu
        FROM recipe_tags_analytics
        WHERE tag_name IN ({placeholders})
        ORDER BY week_start_date ASC, tag_name
    """, top_tags)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No weekly tag data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'tag_name', 'percentage_of_menu'])
    
    fig = px.area(
        df,
        x='week_start_date',
        y='percentage_of_menu',
        color='tag_name',
        title='Recipe Tags Distribution Over Time (Top 8 Tags)',
        labels={'week_start_date': 'Week', 'percentage_of_menu': 'Percentage of Menu', 'tag_name': 'Tag'},
        height=450,
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    
    fig.update_layout(_get_standard_layout(
        hovermode='x unified',
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
    ))
    _apply_grid_styling(fig, x_grid=True, y_grid=True)
    
    print(f"    ✓ Generated with {len(df)} data points from {df['week_start_date'].nunique()} weeks")
    
    chart_html = fig.to_html(include_plotlyjs=False, div_id="recipe_tags_chart")
    return extract_plotly_chart(chart_html)


def generate_ingredient_complexity_chart(conn: sqlite3.Connection) -> str:
    """Chart: Ingredient complexity metrics - line chart showing ingredient count trends per week.
    
    Tracks the average number of ingredients per recipe over time.
    """
    if not HAS_PLOTLY:
        return ""
    
    print("  → Generating ingredient complexity chart...")
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            week_start_date,
            avg_ingredients_per_recipe,
            min_ingredients,
            max_ingredients
        FROM ingredient_complexity_metrics
        ORDER BY week_start_date ASC
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("    ⚠️  No ingredient complexity data available")
        return ""
    
    df = pd.DataFrame(rows, columns=['week_start_date', 'avg_ingredients_per_recipe', 'min_ingredients', 'max_ingredients'])
    
    fig = go.Figure()
    
    # Add average line
    fig.add_trace(go.Scatter(
        x=df['week_start_date'],
        y=df['avg_ingredients_per_recipe'],
        name='Average Ingredients per Recipe',
        mode='lines+markers',
        line=dict(color=COLOR_PRIMARY, width=3),
        marker=dict(size=7),
    ))
    
    # Add range as shaded area (max - min)
    fig.add_trace(go.Scatter(
        x=df['week_start_date'],
        y=df['max_ingredients'],
        fill=None,
        mode='lines',
        line_color='rgba(0,0,0,0)',
        showlegend=False,
        name='Max',
    ))
    
    fig.add_trace(go.Scatter(
        x=df['week_start_date'],
        y=df['min_ingredients'],
        fill='tonexty',
        mode='lines',
        line_color='rgba(0,0,0,0)',
        name='Min-Max Range',
        fillcolor='rgba(46, 134, 171, 0.2)',
    ))
    
    fig.update_layout(_get_standard_layout(
        title='Ingredient Complexity Metrics Over Time',
        xaxis_title='Week',
        yaxis_title='Number of Ingredients',
        hovermode='x unified',
        height=450,
    ))
    _apply_grid_styling(fig, x_grid=True, y_grid=True)
    
    print(f"    ✓ Generated ingredient complexity data for {len(df)} weeks")
    
    chart_html = fig.to_html(include_plotlyjs=False, div_id="ingredient_complexity_chart")
    return extract_plotly_chart(chart_html)


def generate_summary_metrics_panel(conn: sqlite3.Connection) -> str:
    """Generate summary metrics cards showing key aggregates.
    
    Creates an HTML panel with 4 metric cards displaying key dashboard statistics.
    """
    print("  → Generating summary metrics panel...")
    
    metrics = get_summary_metrics(conn)
    
    html_parts = []
    html_parts.append('<div class="metrics-grid">')
    
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
