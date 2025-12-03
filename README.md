# Scraper

`catalog_builder.py` builds a spare-parts catalog from eBay for major European brands and models.

## Running

```bash
python catalog_builder.py [--proxy http://user:pass@host:port]
```

The script:
- Fetches model lists for every configured brand from public sources.
- Queries eBay with OEM-focused searches for a fixed list of parts.
- Extracts pricing, currency, images, OEM references, and listing URLs.
- Resumes from an existing `catalog.json` without deleting prior results and appends OEM hits to `log.txt`.

Results are written to `catalog.json` using the brand → model → part tree requested in the task description.
