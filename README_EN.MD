# Daily Sales Processing – Cash Reconciliation

🇺🇸 This README is written in English.  
🇧🇷 Leia a versão em português: [README.md](README.md)

---

## Overview

This Python script was developed to support **daily cash reconciliation for restaurants within the NORI Group**, automating the processing of sales reports exported from the Cielo system. The project addresses an operational limitation of the platform, which does not allow sales values to be easily summed and classified by **payment method** and **sales channel**.

The script cleans, standardizes, and aggregates raw transaction data to generate a **reliable daily summary**, reducing manual effort, minimizing reconciliation errors, and improving the efficiency of daily financial closing.

---

## Business Context

CSV reports exported from the Cielo system typically present several challenges, such as:
- inconsistent or non-standard headers  
- monetary values stored as text  
- lack of aggregation by payment method (Pix, Credit, Debit)  
- limited support for fast and accurate cash reconciliation  

This script transforms raw transactional data into a clear, structured analytical table ready for administrative and financial validation.

---

## Features

- Dynamic detection of the actual table header in CSV files  
- Data cleaning and conversion of dates and monetary values (Brazilian format)  
- Filtering by specific establishment  
- Automatic classification of payment methods (Pix, Credit, Debit)  
- Sales channel normalization (Machine / TEF)  
- Daily aggregation by date and sales channel  
- Automatic calculation of totals  
- Export of a consolidated daily summary CSV  
- Automatic organization of processed files  

---

## Technologies

- Python  
- Pandas  
- Pathlib  

---

## How to Use

1. Place the CSV files exported from the Cielo system into the `entrada_diaria/` folder  
2. Run the script:
   ```bash
   python processamento_vendas.py
