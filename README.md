# NFL Madden Data Analytics Platform

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://nfl-madden-app.streamlit.app/)

A comprehensive data platform for analyzing NFL player ratings from Madden 02 to present, combining historical Madden ratings with real NFL performance metrics.

## 🎮 Overview

This project provides a robust pipeline for collecting, processing, and analyzing NFL player ratings from Madden games, combined with real NFL performance data. Supporting player data from Madden 02 to current releases, it offers unique insights into player performance and rating evolution.

## ⚡ Features

- **Interactive Web Interface**: Built with Streamlit for easy data exploration
- **Historical Coverage**: Player ratings from Madden 02 to present
- **Advanced Analytics**: Combines game ratings with real NFL performance metrics
- **Automated Data Pipeline**: Regular updates and data processing
- **Comprehensive Player Analysis**: Multiple rating categories and performance metrics

## 🛠️ Data Pipeline Services

### Madden Data Processing

1. **Raw Data Collection**
   - Source: maddenratings.weebly.com
   - Automated scraping and data extraction
   - Raw player ratings and attributes

2. **Stage Processing**
   - Standardization of attribute names
   - Initial data cleaning and formatting
   - Soft imputation of missing ratings

3. **Data Integration**
   - Combination with nflverse player data
   - Metadata matching and verification
   - Enhanced player profiles

4. **Advanced Processing**
   - Machine learning-based imputation
   - AV-based rating predictions
   - Comprehensive data validation

### Performance Metrics

**Approximate Value (AV) Integration**
- Integration with sportsdataverse
- Historical player performance metrics
- Previous season AV correlation with ratings
- Predictive modeling support

## 🚀 Getting Started

### Prerequisites
```bash
python 3.8+
pip
streamlit
pandas
```

### Installation
```bash
git clone https://github.com/theedgepredictor/nfl-madden-data.git
cd nfl-madden-data
pip install -r requirements.txt
```

### Running the App
```bash
streamlit run app.py
```

## 📊 Data Structure

```
data/
├── madden/
│   ├── raw/       # Original scraped data
│   ├── stage/     # Standardized formats
│   ├── processed/ # Integrated with NFL data
│   └── dataset/   # Final processed datasets
└── pfr/
    └── approximate_value/  # Player performance metrics
```

## 🔗 Links

- [Live Application](https://nfl-madden-app.streamlit.app/)
- [Data Source](https://maddenratings.weebly.com)
