#!/usr/bin/env python3
"""
Generate a demo bank statement PDF based on the NatWest Bankline format.
All EBP transactions are summarised into a single row.
Narratives limited to first 2 lines.
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.lib.colors import black, white, HexColor
from pathlib import Path
import os

OUTPUT_PATH = Path(__file__).parent / "Demo Bank Statement 27.02.26.pdf"

# Page dimensions
PAGE_W, PAGE_H = A4  # 210mm x 297mm

# Margins
LEFT = 18 * mm
RIGHT = PAGE_W - 15 * mm
TOP = PAGE_H - 15 * mm
BOTTOM = 25 * mm

# Column positions (x coordinates)
COL_DATE = LEFT
COL_NARRATIVE = LEFT + 55
COL_TYPE = LEFT + 265
COL_DEBIT = LEFT + 315
COL_CREDIT = LEFT + 400
COL_BALANCE = RIGHT - 10

# Fonts
FONT_NORMAL = "Helvetica"
FONT_BOLD = "Helvetica-Bold"
FONT_SIZE = 8.5
HEADER_SIZE = 9
LINE_HEIGHT = 13  # points between rows
NARRATIVE_LINE_HEIGHT = 11  # slightly tighter for multi-line narratives


# ============================================================
# Transaction data - extracted from original statement
# ============================================================

# PPY-prefixed EBP debits (34 transactions) → will be summarised
# The 2 named EBP transfers (G&WCT STAFFORDSHIR, GAME & WILDLIFE CO) remain separate
PPY_EBP_DEBITS = [
    64.90, 3858.90, 211.20, 1143.10, 51.61, 16.74, 348.00, 47.50,
    55.91, 508.08, 4841.27, 1428.00,  # page 1 (12)
    2851.38, 871.56, 4964.26, 760.00, 7409.39, 560.00, 38.69, 416.38,
    1694.35, 2045.35, 4571.31, 26.98, 71.18, 2400.00, 773.06, 272.00,  # page 2 (16)
    581.85, 167.02, 25.40, 25.56, 96.00, 2500.00,  # page 3 (6, excluding 2 named transfers)
]
EBP_TOTAL = round(sum(PPY_EBP_DEBITS), 2)
EBP_COUNT = len(PPY_EBP_DEBITS)

# Transactions in statement order (top=closing balance → bottom=opening balance)
# Format: (date, narrative_lines, type, debit, credit)
# narrative_lines is a list of strings (max 2 lines)

TRANSACTIONS = [
    # D/D debits
    ("27/02/2026", ["XEJ007HVBB2644J09K"], "D/D", 36.16, None),
    ("27/02/2026", ["C38925205206997"], "D/D", 165.60, None),

    # EBP SUMMARY (replaces 34 PPY-prefixed transactions)
    ("27/02/2026", [f"SUPPLIER PAYMENTS ({EBP_COUNT} ITEMS)", "PPY/1571", "FP 27/02/26 40"], "EBP", EBP_TOTAL, None),

    # Named EBP transfers (kept as individual lines)
    ("27/02/2026", ["G&WCT STAFFORDSHIR", "TFR"], "EBP", 2160.00, None),
    ("27/02/2026", ["GAME & WILDLIFE CO", "TFR"], "EBP", 2400.00, None),

    # CHG debits (page 3)
    ("27/02/2026", ["30JAN A/C    5456"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    0688"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7915"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    3553"], "CHG", 6.44, None),
    ("27/02/2026", ["30JAN A/C    3073"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    5519"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    5411"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    2661"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    1495"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    0448"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    8214"], "CHG", 6.22, None),
    ("27/02/2026", ["30JAN A/C    8206"], "CHG", 6.22, None),
    ("27/02/2026", ["30JAN A/C    8117"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    8109"], "CHG", 12.59, None),
    ("27/02/2026", ["30JAN A/C    8028"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7994"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7978"], "CHG", 7.54, None),
    ("27/02/2026", ["30JAN A/C    7951"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7943"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7927"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7889"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7854"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7846"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7811"], "CHG", 6.22, None),
    ("27/02/2026", ["30JAN A/C    7803"], "CHG", 6.22, None),
    ("27/02/2026", ["30JAN A/C    7773"], "CHG", 6.22, None),
    ("27/02/2026", ["30JAN A/C    7765"], "CHG", 6.22, None),
    ("27/02/2026", ["30JAN A/C    7757"], "CHG", 6.00, None),

    # CHG debits (page 4)
    ("27/02/2026", ["30JAN A/C    7749"], "CHG", 6.44, None),
    ("27/02/2026", ["30JAN A/C    7730"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7722"], "CHG", 13.82, None),
    ("27/02/2026", ["30JAN A/C    7714"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7641"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7633"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7595"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7587"], "CHG", 6.66, None),
    ("27/02/2026", ["30JAN A/C    7579"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7560"], "CHG", 6.44, None),
    ("27/02/2026", ["30JAN A/C    7552"], "CHG", 6.44, None),
    ("27/02/2026", ["30JAN A/C    7544"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7536"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7501"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7498"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7471"], "CHG", 6.22, None),
    ("27/02/2026", ["30JAN A/C    7463"], "CHG", 6.22, None),
    ("27/02/2026", ["30JAN A/C    7455"], "CHG", 9.52, None),
    ("27/02/2026", ["30JAN A/C    7439"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7420"], "CHG", 6.00, None),
    ("27/02/2026", ["30JAN A/C    7404"], "CHG", 6.66, None),
    ("27/02/2026", ["30JAN A/C    7005"], "CHG", 394.54, None),

    # BGC credits
    ("27/02/2026", ["002732 605114"], "BGC", None, 4850.00),
    ("27/02/2026", ["002731 605114"], "BGC", None, 25.00),
    ("27/02/2026", ["002730 605114"], "BGC", None, 50.00),
    ("27/02/2026", ["002729 605114"], "BGC", None, 225.00),

    # BAC credit
    ("27/02/2026", ["HEX BACS"], "BAC", None, 100.00),

    # DPC debit
    ("27/02/2026", ["VIA MOBILE - PYMT"], "DPC", 42.74, None),

    # BAC credits (pages 4-5)
    ("27/02/2026", ["FP 27/02/26 0259", "62025808706326000N"], "BAC", None, 30588.92),
    ("27/02/2026", ["FP 27/02/26 0249", "47024913451337000N"], "BAC", None, 233.37),
    ("27/02/2026", ["FP 27/02/26 0253", "33024416087829000N"], "BAC", None, 33765.15),
    ("27/02/2026", ["FP 27/02/26 0256", "63024416625352000N"], "BAC", None, 27985.64),
    ("27/02/2026", ["SPE33674", "FP 27/02/26 0209"], "BAC", None, 12.50),
    ("27/02/2026", ["BCARD5511183260226", "FP 27/02/26 0848"], "BAC", None, 440.00),
    ("27/02/2026", ["FP 27/02/26 1425", "53901431524172200I"], "BAC", None, 3751.80),
    ("27/02/2026", ["FP 27/02/26 0257", "38024948760430000N"], "BAC", None, 9155.03),
    ("27/02/2026", ["STRIPE PAYMENTS UK", "STRIPE"], "BAC", None, 577.09),
    ("27/02/2026", ["DOG001", "FP 27/02/26 0957"], "BAC", None, 21.37),
    ("27/02/2026", ["FP 27/02/26 0259", "25024913811970000N"], "BAC", None, 75136.84),
    ("27/02/2026", ["SIN007155", "FP 27/02/26 1209"], "BAC", None, 100.85),
    ("27/02/2026", ["ASH 17084", "FP 27/02/26 0129"], "BAC", None, 20.00),
    ("27/02/2026", ["BCARD1725814250226", "FP 27/02/26 0849"], "BAC", None, 400.00),
]

# Calculate totals
TOTAL_DEBITS = round(sum(t[3] for t in TRANSACTIONS if t[3]), 2)
TOTAL_CREDITS = round(sum(t[4] for t in TRANSACTIONS if t[4]), 2)

# Opening balance matches Opera nk_recbal for BC060 (£0.00)
OPENING_BALANCE = 0.00
CLOSING_BALANCE = round(OPENING_BALANCE + TOTAL_CREDITS - TOTAL_DEBITS, 2)


def fmt_amount(val):
    """Format amount with commas and 2dp."""
    if val is None:
        return ""
    return f"{val:,.2f}"


def generate_demo_statement(output_path=None):
    if output_path is None:
        output_path = OUTPUT_PATH

    output_path = Path(output_path)
    c = canvas.Canvas(str(output_path), pagesize=A4)

    page_num = [1]
    total_pages = [0]  # will calculate after layout

    # Pre-calculate total pages
    # Header takes ~105pt, each transaction ~13-26pt, footer ~25pt
    usable_height = TOP - BOTTOM - 105  # after header
    # Rough estimate: most transactions are 1 line (13pt), some 2 lines (24pt)
    two_line_count = sum(1 for t in TRANSACTIONS if len(t[1]) > 1)
    one_line_count = len(TRANSACTIONS) - two_line_count
    # Add extra for CLOSING/OPENING balance lines, totals
    total_content_height = (one_line_count * LINE_HEIGHT +
                           two_line_count * (LINE_HEIGHT + NARRATIVE_LINE_HEIGHT) +
                           4 * LINE_HEIGHT)  # closing, opening, totals, spacing

    first_page_usable = usable_height
    continuation_usable = TOP - BOTTOM - 50  # continuation pages have smaller header

    if total_content_height <= first_page_usable:
        total_pages[0] = 1
    else:
        remaining = total_content_height - first_page_usable
        total_pages[0] = 1 + max(1, int(remaining / continuation_usable) + 1)

    def draw_header(is_first_page=True):
        """Draw page header."""
        y = TOP

        if is_first_page:
            # NatWest logo area
            c.setFont(FONT_BOLD, 18)
            c.setFillColor(HexColor("#4a1c7a"))  # NatWest purple
            c.drawString(LEFT, y, "NatWest")
            c.setFillColor(black)

            # Bankline
            c.setFont(FONT_BOLD, 22)
            c.drawRightString(RIGHT, y, "Bankline")

            y -= 18
            # Statement line
            c.setFont(FONT_NORMAL, HEADER_SIZE)
            c.drawString(LEFT, y, "Statement for account 54-99-45   99997005 from 27/02/2026 to 27/02/2026")

            y -= 5
            c.setStrokeColor(black)
            c.setLineWidth(0.5)
            c.line(LEFT, y, RIGHT, y)
            y -= 14

            # Account details - left column
            details_left = [
                ("Short name:", "G&WCT REG CHARITY"),
                ("Alias:", "GAME & WILDLIFE CONS"),
                ("BIC:", ""),
                ("IBAN:", ""),
            ]
            details_right = [
                ("Currency:", "GBP"),
                ("Account type:", "BUSINESS CURRENT"),
                ("Bank name:", "National Westminster Bank"),
                ("Bank branch:", "BOURNEMOUTH (F)"),
            ]

            c.setFont(FONT_NORMAL, HEADER_SIZE)
            dy = y
            for label, value in details_left:
                c.setFont(FONT_BOLD, HEADER_SIZE - 0.5)
                c.drawString(LEFT, dy, label)
                c.setFont(FONT_NORMAL, HEADER_SIZE - 0.5)
                c.drawString(LEFT + 55, dy, value)
                dy -= 13

            dy = y
            mid_x = LEFT + 310
            for label, value in details_right:
                c.setFont(FONT_BOLD, HEADER_SIZE - 0.5)
                c.drawString(mid_x, dy, label)
                c.setFont(FONT_NORMAL, HEADER_SIZE - 0.5)
                c.drawString(mid_x + 70, dy, value)
                dy -= 13

            y = dy - 5
            c.setLineWidth(0.5)
            c.line(LEFT, y, RIGHT, y)
            y -= 14

        else:
            # Continuation header
            c.setFont(FONT_NORMAL, HEADER_SIZE)
            c.drawString(LEFT, y, "Statement for account 54-99-45   99997005 from 27/02/2026 to 27/02/2026")
            y -= 14

        # Column headers
        c.setFont(FONT_BOLD, HEADER_SIZE)
        c.drawString(COL_DATE, y, "Date")
        c.drawString(COL_NARRATIVE, y, "Narrative")
        c.drawString(COL_TYPE, y, "Type")
        c.drawRightString(COL_DEBIT + 45, y, "Debit")
        c.drawRightString(COL_CREDIT + 45, y, "Credit")
        c.drawRightString(COL_BALANCE + 20, y, "Ledger balance")

        y -= 3
        c.setLineWidth(0.5)
        c.line(LEFT, y, RIGHT, y)
        y -= LINE_HEIGHT

        return y

    def draw_footer():
        """Draw page footer."""
        fy = BOTTOM - 10
        c.setLineWidth(0.3)
        c.line(LEFT, fy + 5, RIGHT, fy + 5)

        c.setFont(FONT_NORMAL, 7)
        c.drawString(LEFT, fy - 5,
                      "NB: Transactions with today's or next business day's date may still be subject to "
                      "confirmation and may subsequently be reversed from your account.")
        c.drawString(LEFT, fy - 15,
                      f"Printed on 02/03/2026 at 07:30 by user C0RP0RATE2")
        c.drawRightString(RIGHT, fy - 15,
                          f"Page {page_num[0]} of {total_pages[0]}")

    def new_page(is_first=False):
        if not is_first:
            draw_footer()
            c.showPage()
            page_num[0] += 1

    def check_space(y, needed):
        """Check if we have enough space, start new page if not."""
        if y - needed < BOTTOM + 15:
            # Draw BALANCE BROUGHT FORWARD before page break
            c.setFont(FONT_BOLD, FONT_SIZE)
            c.drawString(COL_NARRATIVE, y, "BALANCE BROUGHT FORWARD")
            draw_footer()
            c.showPage()
            page_num[0] += 1
            y = draw_header(is_first_page=False)
            c.setFont(FONT_BOLD, FONT_SIZE)
            c.drawString(COL_NARRATIVE, y, "BALANCE CARRIED FORWARD")
            y -= LINE_HEIGHT
            return y
        return y

    # ============================================================
    # Generate PDF
    # ============================================================

    # Page 1
    y = draw_header(is_first_page=True)

    # CLOSING BALANCE
    c.setFont(FONT_BOLD, FONT_SIZE)
    c.drawString(COL_NARRATIVE, y, "CLOSING BALANCE")
    c.setFont(FONT_NORMAL, FONT_SIZE)
    c.drawRightString(COL_BALANCE + 20, y, fmt_amount(CLOSING_BALANCE))
    y -= LINE_HEIGHT

    # Draw all transactions
    for date, narrative_lines, ttype, debit, credit in TRANSACTIONS:
        # Calculate space needed for this transaction
        lines_needed = max(1, len(narrative_lines))
        space_needed = LINE_HEIGHT + (lines_needed - 1) * NARRATIVE_LINE_HEIGHT

        y = check_space(y, space_needed + LINE_HEIGHT)

        # Date
        c.setFont(FONT_NORMAL, FONT_SIZE)
        c.drawString(COL_DATE, y, date)

        # Narrative (first line on same row as date)
        if narrative_lines:
            c.drawString(COL_NARRATIVE, y, narrative_lines[0])

        # Type
        c.drawString(COL_TYPE, y, ttype)

        # Debit
        if debit is not None:
            c.drawRightString(COL_DEBIT + 45, y, fmt_amount(debit))

        # Credit
        if credit is not None:
            c.drawRightString(COL_CREDIT + 45, y, fmt_amount(credit))

        # Additional narrative lines
        for i in range(1, len(narrative_lines)):
            y -= NARRATIVE_LINE_HEIGHT
            c.drawString(COL_NARRATIVE, y, narrative_lines[i])

        y -= LINE_HEIGHT

    # OPENING BALANCE
    y = check_space(y, LINE_HEIGHT * 3)
    c.setFont(FONT_BOLD, FONT_SIZE)
    c.drawString(COL_NARRATIVE, y, "OPENING BALANCE")
    c.setFont(FONT_NORMAL, FONT_SIZE)
    c.drawRightString(COL_BALANCE + 20, y, fmt_amount(OPENING_BALANCE))
    y -= LINE_HEIGHT * 1.5

    # Totals
    y = check_space(y, LINE_HEIGHT * 2)
    c.setLineWidth(0.5)
    c.line(LEFT, y + 5, RIGHT, y + 5)
    y -= 3

    c.setFont(FONT_BOLD, FONT_SIZE)
    c.drawString(LEFT, y, "Totals")
    c.setFont(FONT_NORMAL, FONT_SIZE)
    c.drawRightString(COL_DEBIT + 45, y, fmt_amount(TOTAL_DEBITS))
    c.drawRightString(COL_CREDIT + 45, y, fmt_amount(TOTAL_CREDITS))

    # Update total pages now we know the actual count
    actual_pages = page_num[0]

    # Final footer
    draw_footer()
    c.save()

    # If page count was wrong, regenerate with correct count
    if actual_pages != total_pages[0]:
        total_pages[0] = actual_pages
        c = canvas.Canvas(str(output_path), pagesize=A4)
        page_num[0] = 1

        # Regenerate with correct page count
        y = draw_header(is_first_page=True)

        c.setFont(FONT_BOLD, FONT_SIZE)
        c.drawString(COL_NARRATIVE, y, "CLOSING BALANCE")
        c.setFont(FONT_NORMAL, FONT_SIZE)
        c.drawRightString(COL_BALANCE + 20, y, fmt_amount(CLOSING_BALANCE))
        y -= LINE_HEIGHT

        for date, narrative_lines, ttype, debit, credit in TRANSACTIONS:
            lines_needed = max(1, len(narrative_lines))
            space_needed = LINE_HEIGHT + (lines_needed - 1) * NARRATIVE_LINE_HEIGHT
            y = check_space(y, space_needed + LINE_HEIGHT)

            c.setFont(FONT_NORMAL, FONT_SIZE)
            c.drawString(COL_DATE, y, date)
            if narrative_lines:
                c.drawString(COL_NARRATIVE, y, narrative_lines[0])
            c.drawString(COL_TYPE, y, ttype)
            if debit is not None:
                c.drawRightString(COL_DEBIT + 45, y, fmt_amount(debit))
            if credit is not None:
                c.drawRightString(COL_CREDIT + 45, y, fmt_amount(credit))
            for i in range(1, len(narrative_lines)):
                y -= NARRATIVE_LINE_HEIGHT
                c.drawString(COL_NARRATIVE, y, narrative_lines[i])
            y -= LINE_HEIGHT

        y = check_space(y, LINE_HEIGHT * 3)
        c.setFont(FONT_BOLD, FONT_SIZE)
        c.drawString(COL_NARRATIVE, y, "OPENING BALANCE")
        c.setFont(FONT_NORMAL, FONT_SIZE)
        c.drawRightString(COL_BALANCE + 20, y, fmt_amount(OPENING_BALANCE))
        y -= LINE_HEIGHT * 1.5

        y = check_space(y, LINE_HEIGHT * 2)
        c.setLineWidth(0.5)
        c.line(LEFT, y + 5, RIGHT, y + 5)
        y -= 3
        c.setFont(FONT_BOLD, FONT_SIZE)
        c.drawString(LEFT, y, "Totals")
        c.setFont(FONT_NORMAL, FONT_SIZE)
        c.drawRightString(COL_DEBIT + 45, y, fmt_amount(TOTAL_DEBITS))
        c.drawRightString(COL_CREDIT + 45, y, fmt_amount(TOTAL_CREDITS))

        draw_footer()
        c.save()

    print(f"Generated: {output_path}")
    print(f"  EBP summary: {EBP_COUNT} transactions → 1 row, total £{fmt_amount(EBP_TOTAL)}")
    print(f"  Total debits: £{fmt_amount(TOTAL_DEBITS)}")
    print(f"  Total credits: £{fmt_amount(TOTAL_CREDITS)}")
    print(f"  Opening balance: £{fmt_amount(OPENING_BALANCE)}")
    print(f"  Closing balance: £{fmt_amount(CLOSING_BALANCE)}")
    print(f"  Pages: {total_pages[0]}")


if __name__ == "__main__":
    generate_demo_statement()
