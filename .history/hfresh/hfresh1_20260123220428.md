
https://console.hfresh.info/docs/get-started

https://console.hfresh.info/docs

How to interact with api data?

Using "Fetch Web Page"

Fetched 2 resources

From the hfresh API documentation, here's what you can obtain:

**Main Data Resources:**

1. **Recipes**
   - List recipes (with search/filter capabilities)
   - Get individual recipe details

2. **Menus**
   - List weekly menus
   - Get individual menu details with recipes

3. **Reference Data**
   - **Tags** – Recipe categories and tags
   - **Labels** – Recipe labels (dietary, preparation style, etc.)
   - **Allergens** – Allergen information
   - **Ingredients** – Available ingredients list
   - **Countries** – Supported locales and countries

**Key API Details:**
- **Rate limit:** 60 requests/minute per user
- **Pagination:** 10-200 results per page (default 50)
- **Localization:** Supports multiple locales (e.g., en-GB, de-DE) for language/region-specific data
- **Authentication:** Bearer token required
- **Format:** JSON responses

**Available Downloads:**
- OpenAPI 3.0 (Swagger) specification
- Postman collection

This is a **HelloFresh recipe database API** that provides access to recipes, menus, and associated metadata like ingredients, allergens, and tags. You can build a comprehensive food/recipe data warehouse from this.