# Gandhi & Bros Co. — NSE indices, ETFs and IPO watch

A responsive Indian market dashboard. The data tables use compact column sizing, tighter cell spacing and sticky identifier columns to reduce horizontal scrolling while retaining the existing font sizes.

Features:

- All indices from NSE's live-market feed, grouped by NSE category. The sortable table includes current value, % change, open/high/low, previous close, previous-day/1-week/1-month/1-year values, and 52-week high/low. Historical headings show dates supplied by NSE. Search, category/move filters, saved indices and CSV export are included.
- Benchmark cards for Nifty 50, Sensex, India VIX, GIFT Nifty, India 24K gold (₹/10g), MCX Silver futures (₹/kg), and Bitcoin (INR). GIFT Nifty comes from NSE market status; gold is Goodreturns’ daily reference rate; silver is Groww’s timestamped live MCX futures quote and refreshes every 10 seconds; Bitcoin shows CoinGecko INR price and rolling 24-hour change/range.
- Five focused views are available from the top navigation: News (four Moneycontrol English/Hindi sections, five linked headlines each), Index (benchmark cards and trend chart), Indices (the full NSE table), ETFs and IPO Watch. The separate pages avoid one long scroll and give each data section the full workspace.
- A selectable NSE index trend chart above the index table, plotting NSE 1Y/1M/1W/previous-day/latest snapshots. The full [NSE Charting](https://charting.nseindia.com/) tool opens in a new tab because NSE blocks embedding it in another site.
- An NSE ETF market-watch table with 18 fields: symbol, underlying asset, category, sub-category, open, high, low, previous close, LTP, change, % change, volume, value in ₹ crores, i-NAV, NAV, 52-week high/low and 30-day % change. Column sorting (max/min), gainers/losers/saved filters, search, category/sub-category filters, CSV export and manual refresh are included. ETF data refreshes every 30 seconds.
- The complete current IPO GMP table from InvestorGain, including all source rows/statuses and its 13 published columns (name, GMP, rating, subscription, price, issue size, lot, dates, update time and anchor data). Search, market-segment/status filters, sortable headings and CSV export are included; the source refreshes every two minutes.

## Zorin OS / Ubuntu setup

1. Download the updated `marketpulse-dashboard.zip` and extract it into `~/Downloads`. If your browser adds a suffix such as `(1)` to the ZIP name, use that exact filename when extracting.
2. Open Terminal (**Ctrl+Alt+T**). If Python/venv are not installed yet, run this once:

```bash
sudo apt update
sudo apt install -y python3 python3-venv unzip
```

If Python 3 and `python3-venv` are already installed, skip the system-package step.

3. Extract and launch the dashboard. If the ZIP is named differently, replace `marketpulse-dashboard.zip` in the `unzip` command with its actual name.

```bash
cd ~/Downloads
unzip -o marketpulse-dashboard.zip
cd ~/Downloads/marketpulse-dashboard
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
python3 server.py
```

Keep the Terminal open while using the site. When it shows `Gandhi & Bros Co. is serving on 0.0.0.0:4173`, open **http://127.0.0.1:4173**. Use the top navigation or open a direct page: **/news**, **/index**, **/indices**, **/etfs**, or **/ipos** (append the path to the local URL or Render address). Stop the server with **Ctrl+C**.

### Later launches / updated files

If the virtual environment already exists in the project folder, use:

```bash
cd ~/Downloads/marketpulse-dashboard
source .venv/bin/activate
python3 -m pip install -r requirements.txt
python3 server.py
```

After extracting into a new folder (for example `marketpulse-dashboard (2)`), that folder will not have its own `.venv`; create it once there with `python3 -m venv .venv` before activation. Run commands from the folder that contains the current `server.py` and `index.html`. Do not open `index.html` by double-clicking it; start `server.py` first.

## Publish a shareable public link (Render)

The repository includes `render.yaml` and `.python-version` for a Render web service. The app reads Render's `PORT` variable and binds to `0.0.0.0` automatically.

1. Create a GitHub repository. Choose **Private** if you don't want the source code itself to be public; Render can deploy from a private repository after you grant it access.
2. Upload the project files to the repository root: `index.html`, `server.py`, `requirements.txt`, `README.md`, `render.yaml`, `.python-version`, and `.gitignore`.
3. Sign in to Render, choose **New → Blueprint**, connect the GitHub repository, then apply the `gandhi-bros-co-market` web service.
4. When deployment finishes, Render shows the service's `https://…onrender.com` address. Share that HTTPS address with your friend; their phone or computer only needs a browser.

The configured free service is public and read-only; anyone with the URL can open it. Render's free web services spin down after 15 minutes without traffic, so the first visit after a quiet period may take about a minute to wake up. Public market-data feeds can be delayed, throttled, or temporarily unavailable.

## Data sources and limitations

- **Index table:** all entries returned by NSE's public `allIndices` endpoint, the live feed used by the [NSE live market indices page](https://www.nseindia.com/market-data/live-market-indices). NSE's category metadata groups the table. The displayed list is NSE-only; the Sensex benchmark card uses BSE's public quote endpoint.
- **Benchmark cards:** GIFT Nifty is read from NSE's public `marketStatus` response. Gold is the [Goodreturns India 24K reference rate](https://www.goodreturns.in/gold-rates/) per 10g (daily). Silver is the [Groww MCX Silver futures quote](https://groww.in/commodities/futures/mcx_silver), in ₹/kg, with Groww’s source timestamp shown; the dashboard refreshes it every 10 seconds. Bitcoin/INR and its rolling 24-hour range are from [CoinGecko](https://www.coingecko.com/en/coins/bitcoin). Gold and Bitcoin benchmark data are cached for five minutes. These are public reference feeds, not a licensed or guaranteed tick-by-tick service.
- **Moneycontrol headlines:** five headlines each from [News](https://www.moneycontrol.com/news/news-all/), [Business](https://www.moneycontrol.com/news/business/), [Market](https://www.moneycontrol.com/news/business/markets/) and [Hindi News](https://hindi.moneycontrol.com/news/latest-news). Headlines link directly to the matching Moneycontrol site; article text is not copied. The source pages refresh every two minutes; check the publisher’s terms before commercial redistribution.
- **ETF table:** the NSE page-backed `getETFData` feed from the [Exchange Traded Funds page](https://www.nseindia.com/market-data/exchange-traded-funds-etf). The server converts traded value from rupees to crores and caches the feed for 30 seconds.
- **Charts:** the link opens NSE Charting in a new browser tab.
- **IPO GMP table:** every row returned in InvestorGain's public [IPO GMP report](https://www.investorgain.com/report/ipo-gmp-live/331/), with source status (upcoming, open, closing today, closed or listed) and all 13 published table fields. The server caches the page for two minutes.
- Public feeds can be delayed, throttled or unavailable. IPO GMP is an unofficial, informal and unregulated grey-market estimate—not an exchange quote, assured listing gain or investment recommendation. This dashboard is not an exchange-certified tick-by-tick feed; use a licensed provider for guaranteed real-time/commercial trading use.
