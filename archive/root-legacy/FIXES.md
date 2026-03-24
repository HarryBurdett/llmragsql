# SQL RAG - Completed Fixes & Features

This file tracks completed implementations to prevent regressions during development sessions.

---

## User Authentication & Authorization

### Login System
- [x] Login page with Crakd.AI branding (`frontend/src/pages/Login.tsx`)
- [x] Username/password authentication
- [x] Company selection dropdown on login
- [x] License/Client selection (if multiple licenses)
- [x] User's default company pre-selected and marked "(Default)"
- [x] Fetches user's default company on username blur (`/api/auth/user-default-company`)

### Session Management
- [x] JWT-style token authentication
- [x] Token stored in localStorage
- [x] Auth middleware validates tokens (`api/auth_middleware.py`)
- [x] Public paths defined (login, licenses, companies/list, health, docs)

### User Management (`frontend/src/pages/UserManagement.tsx`)
- [x] List all users with status, permissions, last login
- [x] Create new users with username, password, display name, email
- [x] Edit existing users
- [x] Admin toggle (full access to all modules)
- [x] Module permissions checkboxes (cashbook, payroll, ap_automation, utilities, development, administration)
- [x] Reveal password button for admins
- [x] Sync from Opera button
- [x] Default company shown as read-only (synced from Opera)

### Company Access Control
- [x] `user_companies` table in users.db stores per-user company access
- [x] Empty list = access to all companies (no restrictions)
- [x] Admins automatically have access to all companies
- [x] Company Access modal in User Management (view-only, synced from Opera)
- [x] Shows count of companies user can access
- [x] Shows "Default" badge for user's default company

### Opera User Sync (`/api/admin/users/sync-from-opera`)
- [x] Syncs users from Opera's `sequser` table
- [x] Maps Opera NavGroups to SQL RAG module permissions
- [x] Syncs `prefcomp` field as default_company
- [x] Parses `cos` field for company access (characters A-Z, 0-9 map to company database suffixes)
- [x] Creates user if not exists, updates if exists
- [x] Default password = username for new users

### Backend User Auth (`sql_rag/user_auth.py`)
- [x] SQLite database for users, sessions, permissions, user_companies
- [x] Password hashing with PBKDF2-SHA256
- [x] Password encryption for admin reveal feature
- [x] `sync_user_from_opera()` - syncs all Opera-controlled fields
- [x] `get_user_companies()` - get list of company IDs user can access
- [x] `set_user_companies()` - set company access
- [x] `user_has_company_access()` - check if user can access specific company
- [x] `get_user_accessible_companies()` - filter companies list by user access

---

## Bank Statement Reconciliation

### Statement Processing
- [x] Process PDF bank statements using Gemini Vision AI
- [x] Extract transactions with date, description, amount
- [x] Match transactions against Opera ledgers (customers/suppliers)

### Opera Status Display (replaced Gemini-extracted statement info)
- [x] Shows "Last Reconciled Position (Opera)" instead of Gemini-extracted data
- [x] Displays reconciled_balance, current_balance, last_statement_number, last_reconciliation_date
- [x] Data comes from Opera's `nbank` table via `get_bank_reconciliation_status()`

### State Cleanup After Reconciliation
- [x] `statementResult` cleared after successful reconciliation
- [x] `statementPath` cleared after successful reconciliation
- [x] `processingError` cleared after successful reconciliation
- [x] Applies to both `markReconciledMutation.onSuccess` and `handleCompleteReconciliation`

---

## Bank Statement Import (Imports Page)

### PDF Viewer Popup (`frontend/src/pages/Imports.tsx`)
- [x] PDF viewer modal for email attachments (base64 data)
- [x] PDF viewer modal for filesystem PDFs (direct URL via `/api/file/view`)
- [x] View button on email attachments opens PDF in popup
- [x] View button on PDF upload list opens PDF in popup
- [x] Opens directly without confirmation prompt
- [x] 90% viewport width/height modal with close button

### Email Attachment Handling
- [x] `handleEmailAttachmentRawPreview()` detects PDF and shows in popup
- [x] Non-PDF files show raw text preview
- [x] Backend returns `is_pdf: true` and `pdf_data` (base64) for PDFs

### IMAP Provider (`api/email/providers/imap.py`)
- [x] `get_attachment_content()` method added (wraps async `download_attachment`)

### File Format Detection (`frontend/src/pages/Imports.tsx`)
- [x] `handleBankPreview()` routes PDF files to `preview-from-pdf` endpoint instead of `preview-multiformat`
- [x] Default format detection based on file extension when backend doesn't specify
- [x] Email attachment preview also detects PDF files for correct format display
- [x] Preview bar shows "Format: PDF" for PDF files, "Format: CSV" for CSV files

---

## Company Management

### Company Page (`frontend/src/pages/Company.tsx`)
- [x] Shows current company with "Active" badge
- [x] Lists all available companies
- [x] Click to switch company
- [x] Discover Companies button (admin only) - auto-detects Opera installations

### Company Filtering by User Access
- [x] `/api/companies` returns only companies user can access
- [x] `/api/companies/list` returns unfiltered list (for admin use)
- [x] Login page company dropdown filtered by user access

---

## File Locations Reference

| Feature | Files |
|---------|-------|
| Login | `frontend/src/pages/Login.tsx`, `api/main.py` |
| User Management | `frontend/src/pages/UserManagement.tsx`, `api/main.py` |
| User Auth Backend | `sql_rag/user_auth.py` |
| Auth Middleware | `api/auth_middleware.py` |
| Bank Reconciliation | `frontend/src/pages/BankStatementReconcile.tsx`, `sql_rag/statement_reconcile.py` |
| Bank Import/PDF Viewer | `frontend/src/pages/Imports.tsx` |
| Company Management | `frontend/src/pages/Company.tsx` |
| IMAP Provider | `api/email/providers/imap.py` |

---

## Nominal Ledger (NL) Posting

### Unmatched Statement Lines Table (`frontend/src/pages/BankStatementReconcile.tsx`)
- [x] **Transaction Type dropdown** added directly in each unmatched line row
- [x] Options: Customer, Supplier, NL Posting, Bank Transfer
- [x] **Assign Account column** changes dynamically based on selected type:
  - Customer/Supplier: Shows auto-matched account or "No match"
  - NL Posting/Bank Transfer: Shows "—" (detail entry on auto-rec screen)
- [x] Quick create button enabled for Customer/Supplier when account is selected
- [x] NL Posting/Bank Transfer: categorized only, detail added on auto-rec screen
- [x] Per-line state tracking with `lineOverrides` for transaction type
- [x] **Account validation for auto-allocate**: Customer/Supplier lines must have account assigned
  - Yellow background for lines missing account assignment
  - Green background for lines with account assigned
  - Warning message when lines need account assignment
  - "Create All Matched" only includes Customer/Supplier lines with accounts

### Create Entry Modal (Alternative Method)
- [x] "Nominal" entry type button enabled (no longer disabled)
- [x] Nominal account dropdown populated from `/api/gocardless/nominal-accounts`
- [x] When "Nominal" selected, shows nominal account dropdown instead of customer/supplier input
- [x] Correctly determines transaction type (nominal_payment for money out, nominal_receipt for money in)
- [x] Info box explains NL posting purpose (bank charges, interest, etc.)

### Backend Support (`sql_rag/opera_sql_import.py`)
- [x] New `import_nominal_entry()` method added
- [x] Creates records in: aentry, atran, ntran (2 rows for double-entry)
- [x] Supports both payments (money out) and receipts (money in)
- [x] Properly updates nacnt and nbank balances

### API Endpoint (`api/main.py`)
- [x] `/api/cashbook/create-entry` now supports `account_type: 'nominal'`
- [x] Calls `import_nominal_entry()` for nominal transactions

---

## Statement Data Persistence

### Bank Statement Reconciliation (`frontend/src/pages/BankStatementReconcile.tsx`)
- [x] Statement analysis results persist in sessionStorage (survives navigation, cleared on browser close)
- [x] Matching results persist per bank account
- [x] Validation results persist per bank account
- [x] Data automatically loads when returning to the page
- [x] Switching banks loads that bank's persisted data
- [x] **Clear button** added to manually clear all persisted data and start fresh
- [x] Clear button appears only when there's data to clear

---

## Known Working Behaviors (Do Not Change)

1. **Company access is synced from Opera** - Do not add editable company access in User Management
2. **Default company is synced from Opera** - Display only, not editable in SQL RAG
3. **PDF viewer opens without prompt** - User requested this explicitly
4. **Empty company_access list = all companies** - This is intentional design
5. **NL Posting available in Bank Reconciliation** - Posts directly to nominal without customer/supplier ledger

---

*Last updated: Session covering NL Posting feature for bank reconciliation*
