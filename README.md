# Darkling EPUB Toolkit

A batch EPUB optimiser and metadata cleaner for personal e-reader libraries. Processes a source folder of EPUBs into a cleaned output folder in parallel, or can clean metadata in-place on individual files without touching content.

---

## Editor's Note

This script (and README except for this section) was vibe coded.  It was a test to take some of my existing scripts and functions I would like to have to add it into a script.  Feel free to take a modify the script - like any AI generated script, users should be sure to backup their original files.  Original files by default should not be modified in anyway and it generates clean copies - but to be sure make sure you have backup of your original files. 


---

## ⚠️ What This Script Will Change — Read Before Running

This tool makes **permanent, opinionated changes** to your EPUB files. Output files are written to a separate directory in batch mode, so originals are preserved — but in `--metadata-only` mode, files are edited **in-place**. Always keep backups.

The changes below happen automatically. If any category is unacceptable for your library, see the [Arguments](#arguments) section for how to disable it.

### Metadata — Titles

| What changes | Example |
|---|---|
| ALL-CAPS titles corrected to Title Case | `THE GREAT GATSBY` → `The Great Gatsby` |
| all-lowercase titles capitalised | `the great gatsby` → `The Great Gatsby` |
| Trailing retail/format noise stripped | `Dune (Retail) [Kindle Edition]` → `Dune` |
| Genre descriptors stripped from end | `Dune — A Science Fiction Novel` → `Dune` |
| `opf:file-as` sort key regenerated | Leading articles stripped: `The`, `A`, `An`, + 10 other languages |

### Metadata — Authors

| What changes | Example |
|---|---|
| Casing corrected | `JOHN SMITH` → `John Smith` |
| Inverted display names fixed | `Smith, John` → `John Smith` (display name only; sort key stays inverted) |
| `opf:file-as` sort key regenerated | Always `Last, First` format |
| Secondary authors demoted | If a book has multiple `dc:creator` entries, only the first (or the one with `role=aut`) stays as creator; the rest become `dc:contributor`. **Nothing is deleted.** |
| Missing MARC roles defaulted | Creator entries with no `opf:role` get `role="aut"` |
| Non-standard role codes normalised | `"author"` → `"aut"`, `"editor"` → `"edt"`, etc. |

### Metadata — Publishers

| What changes | Example |
|---|---|
| ALL-CAPS corrected to Title Case | `PENGUIN BOOKS` → `Penguin Books` |
| Legal suffixes stripped | `Acme Publishing, Inc.` → `Acme Publishing` |
| Verbose suffixes stripped | `Acme Publishing Group` → `Acme` |
| Parentheticals and URLs stripped | `Tor (tor.com)` → `Tor` |
| Imprints consolidated to parent | `Knopf` → `Penguin Random House`, `Del Rey` → `Penguin Random House` |
| **Genre imprints preserved** | `Tor`, `Ace`, `Orbit`, `Baen`, `Gollancz` are kept as-is by default (disable with `PRESERVE_GENRE_IMPRINTS = False` in config) |

> **Note:** Publisher consolidation is aggressive by design. If you prefer to keep imprint names, use `--no-normalize-publisher`.

### Metadata — Subjects / Tags

| What changes | Example |
|---|---|
| Noise tags removed | `Fiction`, `General`, `Ebook`, `Kindle`, `KindleUnlimited`, `Retail`, `Bestseller`, `to-read`, `owned` … |
| BISAC machine codes removed | `FIC000000`, `sci_biology` |
| Multi-word non-hyphenated tags removed | `Good for book club` (kept: `Science-Fiction`, `Post-Apocalyptic`) |
| Duplicates removed (case-insensitive) | |
| Capped at 5 tags | Configurable via `--max-subjects` |

### Metadata — Other Fields

| What changes | Notes |
|---|---|
| HTML stripped from `dc:description` | `<p>A <b>great</b> book</p>` → `A great book` |
| Language codes normalised to BCP 47 | `EN_us` → `en-US`, `eng` → `en` |
| Date fields normalised to ISO 8601 | `01/15/2023` → `2023-01-15` |
| `dcterms:modified` enforced | Exactly one, in ISO 8601 format |
| Identifier cleaned | Only the ISBN is kept; if no ISBN, the first identifier is kept |
| Empty and duplicate `dc:*` elements removed | |
| Vendor metadata stripped | Calibre, iBooks, Kobo, Sigil private `<meta>` tags removed |
| Calibre ratings removed | Logged as a metadata change |
| Calibre series metadata upgraded | `calibre:series` → EPUB3 `belongs-to-collection` |

---

## What the Full Pipeline Does (Batch Mode)

Beyond metadata, the full pipeline rewrites the EPUB structure, styles, fonts, and images. These changes are only applied in batch mode — `--metadata-only` skips all of them.

### Structure & Safety

- Detects DRM-protected files and skips them entirely (never modified)
- Normalises all text file encodings to UTF-8
- Removes authoring-tool bloat: `.DS_Store`, `iTunesMetadata.plist`, `page-map.xml`, Sigil artefacts, Adobe XPGT page templates, Calibre bookmarks
- Validates EPUB structure before writing; repairs where possible; skips if validation fails
- Repairs broken relative paths in OPF manifest entries and HTML `src=`/`href=` attributes
- Removes duplicate spine items
- Detects and marks empty spine documents as `linear="no"` (title pages with no content)
- Removes orphaned files (on disk but missing from OPF manifest)
- Extracts base64 data-URI images embedded in CSS or HTML into real image files
- Creates `mimetype` file if absent (required by EPUB spec)

### Navigation

- Auto-generates EPUB3 NAV document from NCX, or by scanning `h1`/`h2` headings, if missing
- Trims TOC depth to `TOC_MAX_DEPTH` (default: 2 levels — h1 + h2)
- Rebuilds degenerate TOC when entry count falls below `TOC_MIN_ENTRIES` (default: 3)
- Deduplicates NCX and NAV entries
- Injects semantic landmarks nav: cover, toc, bodymatter
- Repairs broken `<guide>` reference hrefs
- Repairs duplicate `id=` attributes and updates all cross-document fragment links

### Fonts

- Deduplicates embedded fonts by content hash
- Adds `font-display: swap` to all `@font-face` rules (improves render performance)
- Validates `@font-face src:` URLs — removes entire `@font-face` block if the font file is missing
- Corrects legacy font MIME types in OPF manifest (`font/opentype` → `font/otf`, etc.)
- **Subsets fonts** to only the Unicode codepoints actually used in the book (requires `fonttools`)
  - Strips hinting (e-readers ignore it)
  - Preserves original format (woff, ttf, otf)
  - Only subsets fonts larger than `FONT_SUBSET_MIN_KB` (default: 10 KB)
  - Skip with `--no-subset-fonts`
- **Converts TTF/OTF → WOFF2** for EPUB3 books (requires `fonttools` + `brotli`)
  - Skip with `--no-woff2`

### CSS

> **CSS changes are the most visually impactful.** The script makes structural and stylistic changes that are intended to improve compatibility across e-readers, but which can alter the intended appearance of the book.

| Change | Notes |
|---|---|
| Multiple stylesheets per document flattened into one | Reduces HTTP overhead on slower devices |
| Identical per-chapter CSS files consolidated into one shared file | |
| Repeated identical inline `<style>` blocks extracted to a shared CSS file | |
| Unused CSS rules removed | Cross-referenced against actual class/id usage in HTML |
| Duplicate declarations removed | Last declaration wins (standard CSS cascade) |
| `!important` stripped from all CSS and inline `style=` attributes | Can change visual priority ordering |
| Junk no-op inline styles stripped | `color: inherit`, `margin: 0`, `font-size: 1em`, `padding: 0`, etc. |
| Selector complexity reduced | Overly deep selectors simplified |
| Hyphenation CSS injected | `hyphens: auto` on paragraph elements |
| Widows/orphans hints injected | `widows: 2; orphans: 2` |
| Page-break hints injected | On heading elements |
| Render-blocking patterns fixed | `@import` in `<body>`, `<style>` tags inside `<body>` moved to `<head>` |
| `background-color` stripped from `body`/`html` rules | Allows e-reader themes (night mode, sepia) to work correctly |
| CSS properties unsafe for e-readers stripped | `position: fixed`, `vh`/`vw` units, etc. |
| **`--kindle` mode** | Also strips `position: absolute`, `float`, and other layout properties that break Kindle's renderer |
| **`--inject-justify`** (opt-in) | Adds `text-align: justify` to paragraph rules |
| **`--convert-font-units`** (opt-in) | Converts `pt`/`px` font sizes to `em` |

### HTML

| Change | Notes |
|---|---|
| XHTML well-formedness repaired | Self-closing tags, attribute quoting, unclosed elements |
| `charset` meta enforced | `<meta charset="utf-8">` |
| `xml:lang` and `lang` attributes enforced on `<html>` element | |
| `loading="lazy"` added to `<img>` tags | |
| `alt=""` added to images missing it | |
| `<script>` tags removed | Inline `on*` event handlers also removed |
| Typography normalised | Mojibake fixed, smart quotes, em-dashes, ellipsis characters |
| Deprecated HTML attributes stripped | `align=`, `bgcolor=`, `border=`, `valign=` |
| `<address>` sanitised | Converted to `<div class="address">` |
| Attribute-only empty `<span>`/`<div>` shells removed | e.g. `<span class="foo"></span>` with no text content |
| `xml:space="preserve"` added to `<pre>` elements | |
| Print artefacts scrubbed | Printer's keys ("First published…"), "acid-free paper" boilerplate, etc. |
| Dead fragment links repaired | `href="file.xhtml#missing-id"` → `href="file.xhtml"` |
| EPUB3 accessibility metadata injected | `schema:accessMode`, `schema:accessibilityHazard`, etc. (disable with `--no-accessibility-metadata`) |

### XHTML Splitting

Large HTML files crash or corrupt on some Kindle firmware (hard limit ~260 KB). The script automatically splits spine documents larger than `XHTML_SPLIT_KB` (default: 256 KB) at heading boundaries into sequentially-named parts, and updates the OPF manifest, spine, NCX, NAV, and all cross-document fragment links accordingly. Files that cannot be split (no heading boundaries found) are left intact and a warning is issued.

### Images (requires Pillow)

| Change | Notes |
|---|---|
| EXIF metadata stripped from JPEGs | GPS, camera model, timestamps, etc. removed |
| CMYK JPEGs converted to RGB | CMYK is not supported by most e-readers |
| JPEGs re-saved at `JPEG_QUALITY` (default: 85) with progressive encoding | Progressive encoding is 2–8% smaller and renders faster |
| PNGs optimised; palette quantization applied for large PNGs | Only for PNGs > `PNG_QUANTIZE_MIN_KB` (default: 20 KB) |
| WebP → JPEG (no alpha) or PNG (with alpha) | WebP is not supported by most e-reader firmware |
| Static GIFs → PNG | Animated GIFs are left unchanged |
| Images resized if larger than `MAX_SIZE` (default: 1600×2560 px) | `width=`/`height=` attributes in HTML updated to match |

### Packaging

- Repackages the ZIP with `mimetype` as the first file, uncompressed (required by spec)
- Files smaller than `ZIP_STORED_MAX_BYTES` (default: 512 bytes) stored uncompressed for fast random access
- Remaining files compressed at `ZIP_LEVEL` (default: 6)
- Multiple books processed in parallel across `MAX_WORKERS` (default: 4) CPU workers

---

## Installation

```bash
# Required
pip install tqdm charset-normalizer

# Recommended — enables image processing
pip install Pillow

# Optional — enables font subsetting and WOFF2 conversion
pip install fonttools brotli
```

**Python 3.8+ required.** No other dependencies.

---

## Configuration

Open the script and edit the constants near the top of the file:

```python
SOURCE_DIR   = "/path/to/your/epubs"     # Input folder (batch mode)
OUTPUT_DIR   = "/path/to/output"         # Output folder (batch mode)
JPEG_QUALITY      = 85                   # 1–95; lower = smaller files, more compression
MAX_SIZE          = (1600, 2560)         # Max image dimensions in pixels
MAX_WORKERS       = 4                    # Parallel workers; set to 1 to disable
FONT_SUBSET_MIN_KB = 10                  # Only subset fonts larger than this
TOC_MAX_DEPTH     = 2                    # 2 = h1+h2, 3 = h1+h2+h3
XHTML_SPLIT_KB    = 256                  # Split HTML files larger than this
ZIP_LEVEL         = 6                    # 1 (fast) – 9 (small); 6 is the sweet spot
MAX_SUBJECTS      = 5                    # Max subject tags to keep per book
```

---

## Usage

### Batch optimise (most common)

```bash
python epub_toolkit.py
```

Reads all `.epub` files from `SOURCE_DIR`, writes optimised copies to `OUTPUT_DIR`. Originals are never modified.

### Metadata-only clean (in-place)

```bash
python epub_toolkit.py --metadata-only book.epub
python epub_toolkit.py --metadata-only *.epub
```

Cleans metadata only — no image, CSS, HTML, or font changes. Edits files **in-place**.

### Dry-run preview

```bash
python epub_toolkit.py --metadata-only --dry-run book.epub
```

Shows all metadata changes that would be made, without writing anything. Useful for checking before committing.

### Analysis modes

```bash
python epub_toolkit.py --stats book.epub          # File size, word count, font inventory
python epub_toolkit.py --audit-accessibility book.epub   # Alt text, headings, lang, tables
```

---

## Arguments

### Modes

| Argument | Description |
|---|---|
| `--metadata-only FILE…` | Clean metadata in-place on the given files. No HTML/CSS/image changes. |
| `--dry-run` | Use with `--metadata-only`. Shows what would change without writing. |
| `--stats FILE…` | Report file size, word count, font inventory, and asset breakdown for each file. |
| `--audit-accessibility FILE…` | Report accessibility issues: missing alt text, heading structure, lang attributes, table markup. |

### Run Control

| Argument | Description |
|---|---|
| `-v` | Verbose output. Equivalent to `--log-level verbose`. |
| `--log-level LEVEL` | Controls what appears on screen. See [Log Levels](#log-levels) below. |
| `--log-file` | Appends a timestamped session log to `epubtoolkit.log` in the working directory. The log always captures all levels regardless of `--log-level`. |
| `--no-subset-fonts` | Disable font subsetting. Fonts remain embedded but are not trimmed to used characters. |
| `--no-woff2` | Disable TTF/OTF → WOFF2 conversion. |
| `--no-accessibility-metadata` | Disable injection of EPUB3 accessibility schema metadata. |
| `--kindle` | Enable aggressive CSS cleanup mode. In addition to the standard unsafe-property stripping, also removes `position: absolute`, `float`, and other layout properties that break Kindle's renderer. |

### Opt-in Content Features

These are off by default because they make stylistic choices that not everyone wants.

| Argument | Description |
|---|---|
| `--inject-justify` | Add `text-align: justify` to paragraph CSS rules. Useful if the book's stylesheet doesn't specify alignment. |
| `--convert-font-units` | Convert absolute `pt` and `px` font sizes to relative `em`. Helps books scale correctly with the reader's font-size preference. |
| `--max-subjects N` | Override the maximum number of `dc:subject` tags kept per book. Default: 5. |

### Advanced Overrides

All normalisation is on by default. These flags disable whole categories.

| Argument | Description |
|---|---|
| `--no-clean-metadata` | Disable **all** metadata normalisation. Equivalent to passing all four `--no-normalize-*` flags at once. |
| `--no-clean-html` | Disable **all** HTML/CSS cleanup. Structure, images, and fonts are still processed. |
| `--no-normalize-titles` | Skip title casing correction, trailing-noise stripping, and sort-key generation. |
| `--no-normalize-authors` | Skip author casing, inversion fix, sort-key generation, secondary-author demotion, and MARC role normalisation. |
| `--no-normalize-publisher` | Skip publisher casing, legal-suffix stripping, imprint consolidation, and verbose-suffix removal. |
| `--no-normalize-subjects` | Skip subject deduplication, blocklist filtering, and the subject cap. |
| `--publisher-fuzzy` | **(Off by default)** Enable fuzzy matching to auto-correct obvious publisher name typos (e.g. `HarprCollins` → `HarperCollins`). Uses a conservative similarity threshold. |
| `--remove-fields FIELD…` | Delete specific metadata fields entirely. Example: `--remove-fields dc:rights dc:source` |

---

## Log Levels

By default only `Finished:` and `Skipped:` lines are shown — one line per book. Use `--log-level` to increase verbosity.

| Level | Flag | What you see |
|---|---|---|
| `info` | *(default)* | `Finished: book.epub: 1200→890 KB (25.8%) [7 meta]` — one line per book |
| `note` | `--log-level note` | + `Meta: book.epub — 7 change(s) in content.opf` — metadata change summaries |
| `warning` | `--log-level warning` | + `Warning: book.epub — empty spine document …` — structural problems detected |
| `verbose` | `-v` or `--log-level verbose` | + per-step diagnostics, individual change details |

The log file (when `--log-file` is active) always captures all levels regardless of what is shown on screen.

---

## Understanding the Output

A typical run at default log level looks like:

```
  Finished: dune - frank herbert.epub: 1842→1204 KB (34.6%) [8 meta]
  Finished: foundation - isaac asimov.epub: 934→701 KB (24.9%) [5 meta]
  Skipped:  drm-protected-book.epub — SKIPPED (DRM protected)

────────────────────────────────────────────────
  Done.  47 optimised, 3 skipped.
  Space saved     : 38.4 MB
  Metadata changes: 312
  Warnings        : 14
```

The `[8 meta]` suffix is the number of metadata fields changed in that book's OPF file. If the final summary shows warnings, re-run with `--log-level warning` to see them.

A negative size saving (e.g. `-2.8%`) means the output is slightly larger than the input. This can happen when the original was already well-compressed, or when injected metadata and accessibility markup outweighs the savings from font subsetting.

---

## Frequently Asked Questions

**Will this break my books?**
The script validates EPUB structure before writing the output file. If validation fails, the book is skipped and the original is left untouched. However, CSS and metadata changes are applied without a before/after visual comparison — always keep your originals.

**My publisher names are being collapsed to wrong parent companies.**
Publisher consolidation uses a built-in imprint map. To opt out entirely, use `--no-normalize-publisher`. To keep genre imprints (Tor, Ace, Orbit, Baen, Gollancz) but still consolidate everything else, the `PRESERVE_GENRE_IMPRINTS = True` config default already does this.

**Font subsetting made my book larger.**
This can happen with very small fonts that were already compact. Subsetting adds a small overhead for the subset table. Increase `FONT_SUBSET_MIN_KB` in the config to skip smaller fonts.

**The script removed my book's subjects entirely.**
The subject blocklist is aggressive. Tags like `Fiction`, `General`, and single-word genre labels are removed. Multi-word non-hyphenated tags are also dropped. Use `--no-normalize-subjects` to preserve subjects as-is, or `--max-subjects 10` to allow more through.

**I see `SKIPPED (validation failed)` for a book.**
The book's EPUB structure was too broken to safely write a valid output. The original file is left untouched. Run with `-v` to see the specific validation error.

**DRM-protected books are just skipped.**
Correct. The script never attempts to modify DRM-protected files.
