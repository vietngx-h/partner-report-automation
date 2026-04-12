import pandas as pd
import re
import io
import numpy as np
import email.utils
import imaplib
import smtplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
# print(os.getcwd())
# os.chdir('/Users/nguyenviet/Desktop/Gincor/data-01')

# ==========================================
# 1. CLEANING FUNCTIONS
# ==========================================

def clean_dish_d(file_path):
    print(f"Processing Sales Transactions ({file_path})...")
    
    data_pattern = re.compile(r'^[A-Za-z]\s?,\s?\d{2}-\d{2}-\d{2}')
    cleaned_rows = []
    
    # Adjusted Headers (Removed S_REP)
    # Original: CODE, DATE, INV#, ACC#, S_REP, QTY, LINE, PART#, DESCR, NET, COST, CORE
    # We will read all 12, then drop S_REP later
    
    with open(file_path, 'r', encoding='latin1') as f:
        for line in f:
            line = line.strip()
            if data_pattern.match(line):
                parts = line.split(',')
                # Handle extra commas in Description
                if len(parts) > 12:
                    first = parts[:8]
                    last = parts[-3:]
                    middle = [",".join(parts[8:-3])]
                    cleaned_rows.append(first + middle + last)
                elif len(parts) == 12:
                    cleaned_rows.append(parts)

    headers = ["TRANS_TYPE", "DATE", "INVCE_NUM", "ACC_NUM", "S_REP_DROP", 
               "QTY", "LINE_CODE", "PART_NUM", "DESCR", "NET_PR", "COST_PR", "CORE_PR"]
    
    df = pd.DataFrame(cleaned_rows, columns=headers)
    
    # Drop Sales Rep immediately
    df = df.drop(columns=['S_REP_DROP'])

    # Fix Date
    def fix_date(d):
        if not d: return None
        d = d.replace('/', '-')
        try:
            m, d, y = d.split('-')
            if len(y) == 2:
                y = ("20" + y) if int(y) < 50 else ("19" + y)
            return f"{y}-{m}-{d}"
        except: return None
    df['DATE'] = pd.to_datetime(df['DATE'].apply(fix_date), errors='coerce')

    # Force Numerics (Essential for calculations to work!)
    for col in ['QTY', 'NET_PR', 'COST_PR']:
        df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce').fillna(0)

    # Clean IDs
    df['ACC_NUM'] = df['ACC_NUM'].str.strip()
    df['PART_NUM'] = df['PART_NUM'].str.strip()
    df['LINE_CODE'] = df['LINE_CODE'].str.upper().str.strip()
    
    return df

def clean_custinfo(file_path):
    print(f"Processing Customer List ({file_path})...")
    df = pd.read_csv(file_path, encoding='latin1', dtype=str, on_bad_lines='skip')
    df.columns = df.columns.str.strip()
    
    if 'Acc #' in df.columns:
        df['ACC_NUM'] = df['Acc #'].str.strip()
    
    # Rename for the final report
    # We use 'Bill To' for Name and 'Bill Addr #3' for Location (City/Prov)
    df = df.rename(columns={
        'Bill To': 'CUSTOMER_NAME',
        'Bill Addr #3': 'LOCATION'
    })
    
    # Return only what we need
    cols_to_keep = ['ACC_NUM', 'CUSTOMER_NAME', 'LOCATION']
    return df[[c for c in cols_to_keep if c in df.columns]]

def clean_ireport(file_path):
    print(f"Processing Inventory Master ({file_path})...")
    
    # Header cleaning logic
    with open(file_path, 'r', encoding='latin1') as f:
        lines = f.readlines()
    
    cleaned_lines = []
    header_found = False
    for line in lines:
        clean_content = line.strip().lstrip('\x0c')
        if clean_content.startswith("LINE,PART NO."):
            if not header_found:
                cleaned_lines.append(clean_content + "\n")
                header_found = True
            continue
        cleaned_lines.append(line)

    df = pd.read_csv(io.StringIO("".join(cleaned_lines)), dtype=str, on_bad_lines='skip')
    df.columns = df.columns.str.strip()
    
    if 'PART NO.' in df.columns:
        df['PART_NUM'] = df['PART NO.'].str.strip()
        
    return df[['PART_NUM', 'Description']]

def get_product_codes():
    print("Generating Product Code Mapping...")
    # Paste the list of codes and names here
    data = """CODE,LINE_NAME
ADA,ADAPTALL
AF,ARCTIC FOX
AMN,AUTOMANN
AMP,AMERICAN MOBILE POWER
ARC,ARCTIC SNOWPLOWS
ARM,ARVIN MERITOR
ATL,ATLAS HYDRAULICS INC.
BAR,BARGIN WRECKER SUPPLY
BEA,BEAU-ROC
BER,BERENDSEN FLUID POWER
BEZ,BEZARES
BRA,BRAFASCO
BRC,BEAU-ROC BODY & ACCESSORY
BUY,BUYERS PRODUCTS
CAN,CANADIAN BEARING LTD
CCH,C & C HOSE AND FITTINGS
CHE,CHELSEA PT PARTS
CIH,CI HAN AUTOMOTIVE
CLE,CLERAL CANADA
CMH,CUMMINS HYDRAULICS
CPI,CPI AUTOMATION
CRT,C&R TRANSMISSION
CTE,WRECKER PARTS
CW,CW MILL EQUIPMENT
DEL,DEL EQUIPMENT
DEW,DEWEEZE PRODUCTS
DIH,DIHMOSA
DK2,DETAIL K2 PLOWS
DOG,BUYERS PLOWS AND SPREADER
DOP,DOG WHOLEGOODS PRESEASON
DPI,DRIVE PRODUCTS
DT,DUAL TECH
DTS,DRIVETEC SHOP SUPPLIES
DUR,DUR
DYN,DYNAMATIC TECHNOLOGIES
FAS,FASTENAL
FEI,FLEET ENGINEERS INC
FIS,FISHER PRODUCTS
FLD,FLUIDYNE FLUID POWER
FLU,FLUID HOSE AND COUPLING
GAI,GAIA (SAFEPAW) ENTERPRISE
GAL,GALTECH
GAT,GATECREST INDUSTRIES INC.
GEA,GEAR POWER INC
GIN,GIN
GLO,GLOBAL BIO CHEM TECH
GRE,GREEN LINE
HEN,HENDRICKSON
HM,HEAVY MOTION
IFP,IFPA
IMT,IMT AXLE
LAB,LABOUR
LIF,LIFCO HYDRAULICS LTD.
LNK,LINK MFG
LON,LONDON DRIVE SYSTEMS INC
LUB,LUBECORE INTERNATIONAL IN
MED,MEDAL HYDRAULICS BRAZIL
MET,METARIS
MIN,MINIMIZER
MOU,MOUSER ELECTRONICS
MPF,MP FILTRI
NH,NATIONAL HOSE AND FITTINS
NPC,NEAPCO DRIVELINE
OGU,OGURA
OIL,OIL & GREASES
OMF,OMFB
OMS,OMSI TRANSFERCASE
PAR,PAR TECH
PEN,PENCOM
PFP,PHOENIX FRICTION PRODUCTS
PGN,PARAGON BLOWERS
PHC,PARKER COOLERS
PHF,PARKER HOSE AND FITTINGS
PHO,PHOENIX USA INC.
PKP,PARKER PUMPS
PKV,PARKER VALVES
PPP,PARKER PUMP PARTS
PRO,PROPOWER MFG. INC
PTI,POWER TRAIN INDUSTRIES
PTO,CHELSEA PTO ASSEMBLIES
RAC,RIDE AIR CONTROLS
RDL,ROCKFORD DRIVELINE
REX,REXROTH BOSCH
RPT,RPT MUD FLAPS
RUS,RUSH
RWL,RIDEWELL SUSPENSIONS
SAF,SAFEPLAST NA COMPANY
SAL,SALAMI VALVES
SFP,SOUTHERN FLUID POWER
SHP,SHOP SUPPLIES
SIP,SNOW AND ICE PRODUCT
SNW,SNOW-WAY
SPA,PARKER SPECIAL
SPR,SPICER DRIVELINE & ASSEM.
SS,S&S PARTS
STA,STARLIGHT LIGHTING EQUIPM
STF,STAUFF CANADA
TIM,TIMBREN INDUSTRIES
TIR,TIRES
TOA,TOWING ACCESSORIES
TOC,TOWING CABLES
TOD,TOWING DOLLY & PARTS
TOE,LIGHT ELECTRICAL
TOH,TOWING CHAIN
TOL,TOWING LOCKOUT TOOLS
TOS,TOWING STRAPS
TON,TON EQUIPMENT
TRU,TRUCK REVOLUTION
TSI,TECHSPAN
TT,TECTRAN
TW,TW DISTRIBUTION
ULT,ULT
UNI,UNI-BOND
VMC,VMAC
WAT,WATSON & CHALIN
WEB,WEBASTO THERMO SYSTEMS
WES,WESTERN PRODUCTS
WET,WETLINE
WFC,WEATHERHEAD FLUID CONNECT
WHH,HYDRAULIC HOSE"""
    return pd.read_csv(io.StringIO(data))


# ==========================================
# 2. MAIN EXECUTION
# ==========================================

# ==========================================
# 2. MERGE & CALCULATE
# ==========================================

def main():
    # 1. Load Data
    sales = clean_dish_d('DISH-D.CSV')
    cust = clean_custinfo('CUSTINFO.CSV')
    inv = clean_ireport('IREPORT (1).CSV')
    codes = get_product_codes()

    print("\nMerging Dataframes...")
    
    # 2. Merge Sales + Customers
    master = pd.merge(sales, cust, on='ACC_NUM', how='left')
    master['CUSTOMER_NAME'] = master['CUSTOMER_NAME'].fillna("Unknown/Cash Sale")
    
    # 3. Merge Sales + Inventory Description
    master = pd.merge(master, inv, on='PART_NUM', how='left')
    master['Description'] = master['Description'].fillna(master['DESCR']) # Fallback
    
    # 4. Merge Sales + Line Names
    master = pd.merge(master, codes, left_on='LINE_CODE', right_on='CODE', how='left')
    master['LINE_NAME'] = master['LINE_NAME'].fillna(master['LINE_CODE'])

    print("Calculating Financials...")
    
    # 5. Add Fiscal Year and Period Columns
    # Fiscal year starts Nov 1st
    # Nov 1st 2022 - Oct 31st 2023 = Fiscal 2023
    def get_fiscal_year(date):
        if pd.isna(date):
            return None
        # If month is Nov (11) or Dec (12), fiscal year is next calendar year
        # Otherwise, fiscal year is current calendar year
        if date.month >= 11:
            return date.year + 1
        else:
            return date.year
    
    def get_fiscal_period(date):
        if pd.isna(date):
            return None
        # November = Period 1, December = Period 2, January = Period 3, etc.
        # November is month 11, so we adjust: (month - 11) % 12 + 1
        # Nov (11) -> 1, Dec (12) -> 2, Jan (1) -> 3, Feb (2) -> 4, etc.
        if date.month >= 11:
            return date.month - 10  # Nov=1, Dec=2
        else:
            return date.month + 2  # Jan=3, Feb=4, ..., Oct=12
    
    master['FISCAL_YEAR'] = master['DATE'].apply(get_fiscal_year)
    master['PERIOD'] = master['DATE'].apply(get_fiscal_period)
    
    # 6. Calculations
    master['TOTAL_REVENUE'] = master['QTY'] * master['NET_PR']
    master['TOTAL_COST'] = master['QTY'] * master['COST_PR']
    master['GROSS_PROFIT'] = master['TOTAL_REVENUE'] - master['TOTAL_COST']
    
    master['MARGIN_PERCENT'] = np.where(
        master['TOTAL_REVENUE'] != 0,
        (master['GROSS_PROFIT'] / master['TOTAL_REVENUE']),
        0
    )

    # 7. Organize Final Columns (Including Fiscal Year and Period)
    final_columns = [
        'DATE', 'FISCAL_YEAR', 'PERIOD', 'INVCE_NUM', 'TRANS_TYPE',  # Transaction Info with Fiscal Data
        'CUSTOMER_NAME', 'ACC_NUM', 'LOCATION',         # Customer Info
        'LINE_CODE', 'LINE_NAME', 'PART_NUM', 'Description', # Product Info (Line Code & Name Added)
        'QTY', 'NET_PR', 'COST_PR',                     # Unit Metrics
        'TOTAL_REVENUE', 'TOTAL_COST', 'GROSS_PROFIT', 'MARGIN_PERCENT' # Calculated Metrics
    ]
    
    # Filter to only existing columns and sort
    df_final = master[final_columns].sort_values(by='DATE', ascending=False)
    
    # Format Margin as percentage (round to 4 decimal places, e.g., 0.2500 for 25%)
    df_final['MARGIN_PERCENT'] = df_final['MARGIN_PERCENT'].round(4) 

    # 7. Save
    output_file = 'Master_Sales_Report.csv'
    df_final.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\nSUCCESS! Saved to {output_file}")
    print(f"Total Rows: {len(df_final)}")
    print(f"Total Revenue: ${df_final['TOTAL_REVENUE'].sum():,.2f}")
    
    return df_final

if __name__ == "__main__":
    main()
