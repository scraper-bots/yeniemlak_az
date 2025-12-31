#!/usr/bin/env python3
"""
Real Estate Market Analysis - Chart Generation Script
Generates business-focused visualizations for executive decision-making
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')

# Set style for professional-looking charts
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")
CHARTS_DIR = Path('charts')
CHARTS_DIR.mkdir(exist_ok=True)

# Load data
print("Loading dataset...")
df = pd.read_csv('listings.csv')
print(f"Total listings: {len(df):,}")

# Data cleaning and preparation
print("Preparing data...")

# Clean price data
df['price'] = pd.to_numeric(df['price'], errors='coerce')
df = df[df['price'] > 0]

# Clean area_m2
df['area_m2'] = pd.to_numeric(df['area_m2'], errors='coerce')
df = df[df['area_m2'] > 0]

# Calculate price per square meter
df['price_per_m2'] = df['price'] / df['area_m2']
df = df[df['price_per_m2'] < df['price_per_m2'].quantile(0.99)]  # Remove outliers

# Clean rooms
df['rooms'] = pd.to_numeric(df['rooms'], errors='coerce')

# Parse date
df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y', errors='coerce')
df['month'] = df['date'].dt.to_period('M')
df['year'] = df['date'].dt.year

print(f"Clean dataset: {len(df):,} listings")

# ============================================================================
# CHART 1: Inventory Distribution by Property Type
# ============================================================================
print("Generating Chart 1: Property Type Distribution...")
plt.figure(figsize=(12, 6))
property_counts = df['property_type'].value_counts().head(10)
bars = plt.barh(range(len(property_counts)), property_counts.values, color='#2E86AB')
plt.yticks(range(len(property_counts)), property_counts.index)
plt.xlabel('Number of Listings', fontsize=12, fontweight='bold')
plt.ylabel('Property Type', fontsize=12, fontweight='bold')
plt.title('Market Inventory by Property Type\nWhich property categories dominate our listings?',
          fontsize=14, fontweight='bold', pad=20)
plt.gca().invert_yaxis()

# Add value labels
for i, (idx, val) in enumerate(property_counts.items()):
    plt.text(val + 50, i, f'{val:,}', va='center', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig(CHARTS_DIR / '01_property_type_distribution.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 1 saved")

# ============================================================================
# CHART 2: Average Prices by Property Type
# ============================================================================
print("Generating Chart 2: Price Analysis by Property Type...")
plt.figure(figsize=(12, 6))
avg_prices = df.groupby('property_type')['price'].mean().sort_values(ascending=False).head(10)
bars = plt.barh(range(len(avg_prices)), avg_prices.values / 1000, color='#A23B72')
plt.yticks(range(len(avg_prices)), avg_prices.index)
plt.xlabel('Average Price (Thousands AZN)', fontsize=12, fontweight='bold')
plt.ylabel('Property Type', fontsize=12, fontweight='bold')
plt.title('Average Property Prices by Type\nWhich properties command premium pricing?',
          fontsize=14, fontweight='bold', pad=20)
plt.gca().invert_yaxis()

# Add value labels
for i, (idx, val) in enumerate(avg_prices.items()):
    plt.text(val/1000 + 5, i, f'{val/1000:.0f}K', va='center', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig(CHARTS_DIR / '02_avg_price_by_type.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 2 saved")

# ============================================================================
# CHART 3: Top 15 Regions by Listing Volume
# ============================================================================
print("Generating Chart 3: Geographic Distribution...")
plt.figure(figsize=(12, 7))
top_regions = df['region'].value_counts().head(15)
bars = plt.barh(range(len(top_regions)), top_regions.values, color='#F18F01')
plt.yticks(range(len(top_regions)), top_regions.index)
plt.xlabel('Number of Listings', fontsize=12, fontweight='bold')
plt.ylabel('Region', fontsize=12, fontweight='bold')
plt.title('Top 15 Regions by Listing Volume\nWhere is market activity concentrated?',
          fontsize=14, fontweight='bold', pad=20)
plt.gca().invert_yaxis()

# Add value labels
for i, (idx, val) in enumerate(top_regions.items()):
    plt.text(val + 30, i, f'{val:,}', va='center', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig(CHARTS_DIR / '03_top_regions_volume.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 3 saved")

# ============================================================================
# CHART 4: Average Prices in Top 15 Regions
# ============================================================================
print("Generating Chart 4: Regional Pricing Analysis...")
plt.figure(figsize=(12, 7))
top_15_regions = df['region'].value_counts().head(15).index
region_prices = df[df['region'].isin(top_15_regions)].groupby('region')['price'].mean().sort_values(ascending=False)
bars = plt.barh(range(len(region_prices)), region_prices.values / 1000, color='#6A4C93')
plt.yticks(range(len(region_prices)), region_prices.index)
plt.xlabel('Average Price (Thousands AZN)', fontsize=12, fontweight='bold')
plt.ylabel('Region', fontsize=12, fontweight='bold')
plt.title('Average Prices in Top 15 Regions\nWhich markets command premium pricing?',
          fontsize=14, fontweight='bold', pad=20)
plt.gca().invert_yaxis()

# Add value labels
for i, (idx, val) in enumerate(region_prices.items()):
    plt.text(val/1000 + 3, i, f'{val/1000:.0f}K', va='center', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig(CHARTS_DIR / '04_avg_price_by_region.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 4 saved")

# ============================================================================
# CHART 5: Price Distribution by Room Count
# ============================================================================
print("Generating Chart 5: Room Count Analysis...")
plt.figure(figsize=(12, 6))
room_data = df[df['rooms'].notna() & (df['rooms'] <= 10)]
room_avg = room_data.groupby('rooms')['price'].mean().sort_index()
bars = plt.bar(room_avg.index, room_avg.values / 1000, color='#1D3557', width=0.6)
plt.xlabel('Number of Rooms', fontsize=12, fontweight='bold')
plt.ylabel('Average Price (Thousands AZN)', fontsize=12, fontweight='bold')
plt.title('Average Price by Room Count\nHow does unit size impact pricing?',
          fontsize=14, fontweight='bold', pad=20)
plt.xticks(room_avg.index)

# Add value labels
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height + 5,
             f'{height:.0f}K', ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig(CHARTS_DIR / '05_price_by_rooms.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 5 saved")

# ============================================================================
# CHART 6: Listing Volume Trends Over Time
# ============================================================================
print("Generating Chart 6: Temporal Trends...")
plt.figure(figsize=(14, 6))
monthly_listings = df[df['month'].notna()].groupby('month').size()
monthly_listings.index = monthly_listings.index.to_timestamp()
plt.plot(monthly_listings.index, monthly_listings.values, marker='o',
         linewidth=2.5, markersize=8, color='#E63946')
plt.fill_between(monthly_listings.index, monthly_listings.values, alpha=0.3, color='#E63946')
plt.xlabel('Month', fontsize=12, fontweight='bold')
plt.ylabel('Number of New Listings', fontsize=12, fontweight='bold')
plt.title('Monthly Listing Volume Trends\nHow is inventory supply changing over time?',
          fontsize=14, fontweight='bold', pad=20)
plt.xticks(rotation=45, ha='right')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(CHARTS_DIR / '06_listing_trends.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 6 saved")

# ============================================================================
# CHART 7: Price per Square Meter by Property Type
# ============================================================================
print("Generating Chart 7: Price Efficiency Analysis...")
plt.figure(figsize=(12, 6))
price_per_m2_avg = df.groupby('property_type')['price_per_m2'].mean().sort_values(ascending=False).head(10)
bars = plt.barh(range(len(price_per_m2_avg)), price_per_m2_avg.values, color='#06A77D')
plt.yticks(range(len(price_per_m2_avg)), price_per_m2_avg.index)
plt.xlabel('Average Price per Square Meter (AZN)', fontsize=12, fontweight='bold')
plt.ylabel('Property Type', fontsize=12, fontweight='bold')
plt.title('Price per Square Meter by Property Type\nWhich property types offer best value?',
          fontsize=14, fontweight='bold', pad=20)
plt.gca().invert_yaxis()

# Add value labels
for i, (idx, val) in enumerate(price_per_m2_avg.items()):
    plt.text(val + 15, i, f'{val:.0f}', va='center', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig(CHARTS_DIR / '07_price_per_m2_by_type.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 7 saved")

# ============================================================================
# CHART 8: Sales vs Rentals Distribution
# ============================================================================
print("Generating Chart 8: Transaction Type Analysis...")
plt.figure(figsize=(10, 6))
sale_type_counts = df['sale_type'].value_counts()
bars = plt.bar(range(len(sale_type_counts)), sale_type_counts.values,
               color=['#E76F51', '#2A9D8F', '#E9C46A'])
plt.xticks(range(len(sale_type_counts)), sale_type_counts.index, fontsize=11)
plt.ylabel('Number of Listings', fontsize=12, fontweight='bold')
plt.xlabel('Transaction Type', fontsize=12, fontweight='bold')
plt.title('Inventory Distribution: Sales vs Rentals\nWhat is our market composition?',
          fontsize=14, fontweight='bold', pad=20)

# Add value labels
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height + 50,
             f'{int(height):,}', ha='center', va='bottom', fontsize=11, fontweight='bold')

plt.tight_layout()
plt.savefig(CHARTS_DIR / '08_sales_vs_rentals.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 8 saved")

# ============================================================================
# CHART 9: Image Count vs Price Correlation
# ============================================================================
print("Generating Chart 9: Listing Quality Analysis...")
plt.figure(figsize=(12, 6))
img_price = df.groupby('image_count')['price'].mean()
img_price = img_price[img_price.index <= 30]
plt.plot(img_price.index, img_price.values / 1000, marker='o',
         linewidth=2.5, markersize=7, color='#9B59B6')
plt.xlabel('Number of Images in Listing', fontsize=12, fontweight='bold')
plt.ylabel('Average Price (Thousands AZN)', fontsize=12, fontweight='bold')
plt.title('Impact of Listing Quality on Price\nDo better presentations command higher prices?',
          fontsize=14, fontweight='bold', pad=20)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(CHARTS_DIR / '09_images_vs_price.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 9 saved")

# ============================================================================
# CHART 10: Price Range Distribution
# ============================================================================
print("Generating Chart 10: Market Segmentation...")
plt.figure(figsize=(12, 6))
price_bins = [0, 50000, 100000, 150000, 200000, 300000, 500000, 1000000, df['price'].max()]
price_labels = ['<50K', '50K-100K', '100K-150K', '150K-200K', '200K-300K', '300K-500K', '500K-1M', '>1M']
df['price_range'] = pd.cut(df['price'], bins=price_bins, labels=price_labels)
price_dist = df['price_range'].value_counts().sort_index()
bars = plt.bar(range(len(price_dist)), price_dist.values, color='#3498DB')
plt.xticks(range(len(price_dist)), price_dist.index, rotation=45, ha='right', fontsize=11)
plt.ylabel('Number of Listings', fontsize=12, fontweight='bold')
plt.xlabel('Price Range (AZN)', fontsize=12, fontweight='bold')
plt.title('Market Segmentation by Price Range\nWhich price segments have most inventory?',
          fontsize=14, fontweight='bold', pad=20)

# Add value labels
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height + 30,
             f'{int(height):,}', ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig(CHARTS_DIR / '10_price_range_distribution.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 10 saved")

# ============================================================================
# CHART 11: Room Count Distribution
# ============================================================================
print("Generating Chart 11: Unit Size Distribution...")
plt.figure(figsize=(12, 6))
room_dist = df[df['rooms'].notna() & (df['rooms'] <= 10)]['rooms'].value_counts().sort_index()
bars = plt.bar(room_dist.index, room_dist.values, color='#E74C3C', width=0.6)
plt.xlabel('Number of Rooms', fontsize=12, fontweight='bold')
plt.ylabel('Number of Listings', fontsize=12, fontweight='bold')
plt.title('Inventory Distribution by Unit Size\nWhat unit configurations are most common?',
          fontsize=14, fontweight='bold', pad=20)
plt.xticks(room_dist.index)

# Add value labels and percentages
total = room_dist.sum()
for bar in bars:
    height = bar.get_height()
    pct = (height / total) * 100
    plt.text(bar.get_x() + bar.get_width()/2., height + 50,
             f'{int(height):,}\n({pct:.1f}%)', ha='center', va='bottom',
             fontsize=9, fontweight='bold')

plt.tight_layout()
plt.savefig(CHARTS_DIR / '11_room_distribution.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 11 saved")

# ============================================================================
# CHART 12: Average Price Trends Over Time
# ============================================================================
print("Generating Chart 12: Pricing Trends...")
plt.figure(figsize=(14, 6))
monthly_avg_price = df[df['month'].notna()].groupby('month')['price'].mean()
monthly_avg_price.index = monthly_avg_price.index.to_timestamp()
plt.plot(monthly_avg_price.index, monthly_avg_price.values / 1000,
         marker='s', linewidth=2.5, markersize=8, color='#16A085')
plt.fill_between(monthly_avg_price.index, monthly_avg_price.values / 1000, alpha=0.3, color='#16A085')
plt.xlabel('Month', fontsize=12, fontweight='bold')
plt.ylabel('Average Price (Thousands AZN)', fontsize=12, fontweight='bold')
plt.title('Average Listing Price Trends Over Time\nHow are market prices evolving?',
          fontsize=14, fontweight='bold', pad=20)
plt.xticks(rotation=45, ha='right')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(CHARTS_DIR / '12_price_trends.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 12 saved")

# ============================================================================
# Generate Summary Statistics
# ============================================================================
print("\n" + "="*70)
print("ANALYSIS COMPLETE - Summary Statistics")
print("="*70)
print(f"Total Listings Analyzed: {len(df):,}")
print(f"Average Price: {df['price'].mean():,.0f} AZN")
print(f"Median Price: {df['price'].median():,.0f} AZN")
print(f"Average Price per m²: {df['price_per_m2'].mean():,.0f} AZN")
print(f"Most Common Property Type: {df['property_type'].mode()[0]}")
print(f"Top Region: {df['region'].mode()[0]}")
print(f"Average Images per Listing: {df['image_count'].mean():.1f}")
print("="*70)
print(f"\n✓ All 12 charts saved to '{CHARTS_DIR}' directory")
print("✓ Charts are ready for business presentation\n")
