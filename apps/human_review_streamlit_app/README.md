# Human Review Streamlit App

Streamlit application for reviewing and approving AI-extracted email data from the Scott's Workshop GenAI pipeline.

## Overview

This app provides a human-in-the-loop interface for reviewing email data extracted by AI, validating fields, and taking action (approve or request follow-up information from customers).

## Features

- **Dashboard KPIs**: Real-time counts of pending items by validation status
- **Smart Search**: Filter by email ID, sender, or SAP ID
- **Side-by-side Review**: Original email body alongside extracted and validated fields
- **Live Validation**: Real-time field validation with clear indicators
- **Approve/Reject Workflow**: Full audit trail with user tracking
- **Follow-up Email Generation**: Auto-generate customer follow-up emails for incomplete data
- **Activity Log**: Complete history of all review actions

## Architecture

### Data Flow

```
validation_notebook.py
    ↓
review_queue (Delta table)
    ↓
Human Review App (this app)
    ↓
approved_changes + review_actions + outgoing_emails (Delta tables)
```

### Tables Used

1. **`review_queue`** (READ) - Source data from validation pipeline
2. **`synthetic_emails`** (READ) - Original email bodies for preview
3. **`approved_changes`** (WRITE) - Approved data ready for downstream systems
4. **`review_actions`** (WRITE) - Complete audit trail
5. **`outgoing_emails`** (WRITE) - Follow-up emails to customers

**Note:** The app automatically creates the three write tables if they don't exist.

## Prerequisites

1. **Run the validation notebook** to populate the `review_queue` table:
   - Execute `validation_notebook.py` in your Databricks workspace
   - This creates `review_queue` with validated email data

2. **Databricks SQL Warehouse** - For querying Delta tables

3. **Unity Catalog permissions** (see Permissions section below)

## Local Development Setup

### 1. Install Dependencies

```bash
cd apps/human_review_streamlit_app
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file in the app directory:

```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_WAREHOUSE_ID=your-warehouse-id
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=scotts_genai_workshop
```

To get your warehouse ID:
- Go to SQL Warehouses in your Databricks workspace
- Click on your warehouse
- Copy the ID from the URL or warehouse details

### 3. Run Locally

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

## Databricks Apps Deployment

### 1. Update app.yaml (if needed)

The `app.yaml` file is pre-configured with sensible defaults. Update if you're using different catalog/schema:

```yaml
command: ["streamlit", "run", "app.py"]
env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: sql-warehouse
  - name: DATABRICKS_CATALOG
    value: main
  - name: DATABRICKS_SCHEMA
    value: scotts_genai_workshop
```

### 2. Deploy via Databricks CLI or UI

**Using Databricks CLI:**

```bash
databricks apps deploy apps/human_review_streamlit_app \
  --app-name scotts-email-review \
  --description "Human-in-the-loop email review app"
```

**Using Databricks UI:**
- Navigate to **Apps** in your workspace
- Click **Create App**
- Upload the `apps/human_review_streamlit_app` folder
- Configure warehouse and environment variables as needed

### 3. Set Permissions

The app service principal needs the following permissions:

**Table Permissions:**
```sql
-- Read access to source tables
GRANT SELECT ON TABLE main.scotts_genai_workshop.review_queue TO `service-principal-name`;
GRANT SELECT ON TABLE main.scotts_genai_workshop.synthetic_emails TO `service-principal-name`;

-- Write access to output tables (auto-created if missing)
GRANT MODIFY ON SCHEMA main.scotts_genai_workshop TO `service-principal-name`;

-- Or grant specific table permissions after tables are created
GRANT MODIFY ON TABLE main.scotts_genai_workshop.review_actions TO `service-principal-name`;
GRANT MODIFY ON TABLE main.scotts_genai_workshop.approved_changes TO `service-principal-name`;
GRANT MODIFY ON TABLE main.scotts_genai_workshop.outgoing_emails TO `service-principal-name`;
```

**Warehouse Permissions:**
```sql
GRANT USAGE ON SQL WAREHOUSE your-warehouse-id TO `service-principal-name`;
```

## Usage

### Review Workflow

1. **View Dashboard**: See pending items grouped by validation status
2. **Search/Filter**: Narrow down to specific items using the search box
3. **Select Item**: Choose an email from the queue
4. **Review**: Compare original email with extracted data
5. **Validate**: Check all fields pass validation (✅/❌ indicators)
6. **Take Action**:
   - **If complete:** Click "Confirm & Approve" → Saves to `approved_changes`
   - **If incomplete:** Send follow-up email → Saves to `outgoing_emails`

### Validation Rules

- **SAP ID**: Must match `SAPXXXXXX` format (6 digits)
- **Name**: Must include first and last name
- **Email**: Valid email format required
- **Phone**: 10-digit US phone number, auto-formatted as `(XXX) XXX-XXXX`

### Audit Trail

Every action is logged to `review_actions` with:
- Email ID
- Action type (confirmed / followup_sent)
- Actor email (from user headers or local fallback)
- Old and new values (JSON)
- Timestamp

View the **Activity Log** tab to browse all actions and download as CSV.

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABRICKS_WAREHOUSE_ID` | Yes | - | SQL Warehouse ID for queries |
| `DATABRICKS_CATALOG` | No | `main` | Unity Catalog name |
| `DATABRICKS_SCHEMA` | No | `scotts_genai_workshop` | Schema name |
| `DATABRICKS_HOST` | Local only | - | Workspace URL (for local dev) |
| `DATABRICKS_TOKEN` | Local only | - | Personal access token (for local dev) |

**Note:** When deployed as a Databricks App, authentication is handled automatically. `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are only needed for local development.

## File Structure

```
apps/human_review_streamlit_app/
├── app.py              # Streamlit UI and validation logic
├── data.py             # Database queries and data access
├── requirements.txt    # Python dependencies
├── app.yaml           # Databricks Apps configuration
└── README.md          # This file
```

## Development Notes

### Key Components

- **`app.py`**: Streamlit UI, validation functions, user interactions
- **`data.py`**: All database queries, connection management, table operations
- **Validation Functions**: Copied from `validation_notebook.py` for consistency

### Caching Strategy

- SQL connection: Cached with `@st.cache_resource` (never expires)
- KPI counts: Cached for 30 seconds with `@st.cache_data(ttl=30)`
- Pending items: Cached for 30 seconds with `@st.cache_data(ttl=30)`
- Cache cleared after approve/reject to reflect updates

## Troubleshooting

### Connection Errors

If you see "Failed to connect to Databricks":
1. Check `.env` file has correct values
2. Verify your PAT token is valid and not expired
3. Ensure the SQL Warehouse is running
4. Confirm network connectivity to your workspace

### Missing Tables

The app auto-creates `review_actions`, `approved_changes`, and `outgoing_emails` on startup. If this fails:
- Check that your user/service principal has `CREATE TABLE` or `MODIFY` permission on the schema
- Manually create tables using the schema in `data.py`

### No Items to Review

If you see "No items to review":
- Check that `review_queue` has data (run `validation_notebook.py`)
- Verify your search query isn't filtering everything out
- Check if items were already actioned (they're hidden from the queue)

### Empty review_queue

If `review_queue` doesn't exist or is empty:
1. Run `validation_notebook.py` first to create and populate the table
2. Check that the catalog and schema names match your configuration

## Next Steps

After reviewing and approving data:
1. **SAP/CRM Integration**: Build connectors to consume `approved_changes` table
2. **Email Service**: Connect `outgoing_emails` to SendGrid, Gmail API, or other email service
3. **Batch Operations**: Add batch approve for fully validated items
4. **Notifications**: Alert reviewers when new items arrive in queue
5. **Analytics**: Dashboard for review metrics and turnaround time
6. **Workflow Orchestration**: Integrate with Databricks Workflows for end-to-end automation

## Resources

- [Databricks Apps Documentation](https://docs.databricks.com/dev-tools/databricks-apps/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Databricks SQL Connector](https://docs.databricks.com/dev-tools/python-sql-connector.html)
- [Unity Catalog Permissions](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/)
