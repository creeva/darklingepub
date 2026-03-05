#!/usr/bin/env python3
# epub_toolkit.py — batch EPUB optimiser + metadata cleaner.
# Reads SOURCE_DIR → writes OUTPUT_DIR; or cleans metadata in-place with --metadata-only.
# See README.md for full documentation of changes made and all arguments.
#
# Dependencies:
#   pip install tqdm charset-normalizer   # required
#   pip install Pillow                    # images
#   pip install fonttools brotli          # font subsetting + woff2
#
# Usage:
#   python epub_toolkit.py                              # batch optimise
#   python epub_toolkit.py --metadata-only book.epub    # metadata only, in-place
#   python epub_toolkit.py --metadata-only --dry-run *.epub
#   python epub_toolkit.py --kindle --inject-justify
#   python epub_toolkit.py --log-level note --log-file
#   Version 1.1

import argparse
import os
import re
import sys
import uuid
import hashlib
import zipfile
import shutil
import signal
import tempfile
import concurrent.futures
import base64
import io
import threading
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree as ET

from tqdm import tqdm

try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    HAS_PIL = False
    print("Warning: Pillow not installed. Image processing disabled.\n"
          "         Install with: pip install Pillow", file=sys.stderr)

try:
    from charset_normalizer import from_bytes as detect_encoding
    HAS_CHARSET_NORMALIZER = True
except ImportError:
    HAS_CHARSET_NORMALIZER = False
    print("Warning: charset-normalizer not installed. Encoding detection disabled.\n"
          "         Install with: pip install charset-normalizer", file=sys.stderr)

try:
    from fontTools.subset import main as _fonttools_subset_main
    from fontTools.ttLib import TTFont as _TTFont
    HAS_FONTTOOLS = True
except ImportError:
    HAS_FONTTOOLS = False
    print("Warning: fonttools not installed. Font subsetting disabled.\n"
          "         Install with: pip install fonttools", file=sys.stderr)

try:
    from lxml import etree as _lxml_etree
    from lxml import html as _lxml_html
    HAS_LXML = True
except ImportError:
    HAS_LXML = False  # silent — lxml is optional; improves XHTML repair quality

try:
    import brotli as _brotli_check  # noqa: F401
    HAS_BROTLI = True
except ImportError:
    HAS_BROTLI = False  # silent — brotli is optional; required for woff2 font conversion


# ─── Configuration ─────────────────────────────────────────────────────────────
SOURCE_DIR            = "/path/to/your/epubs"        # input folder
OUTPUT_DIR            = "/path/to/output"            # output folder
JPEG_QUALITY          = 85                           # 1–95
MAX_SIZE              = (1600, 2560)                 # max image dimensions (px)
MAX_WORKERS           = 4                            # parallel workers (1 = off)
FONT_SUBSET_MIN_KB    = 10                           # skip subsetting fonts smaller than this
TOC_MIN_ENTRIES       = 3                            # rebuild TOC below this entry count
TOC_MAX_DEPTH         = 2                            # TOC depth: 2 = h1+h2, 3 = h1+h2+h3
TOC_HEADING_DEPTH     = 2                            # heading scan depth for NAV generation
HTML_SPLIT_WARN_KB    = 260                          # warn if HTML file exceeds this after split
XHTML_SPLIT_KB        = 256                          # split XHTML files larger than this
ZIP_LEVEL             = 6                            # 1 (fast) – 9 (small); 6 is the sweet spot
ZIP_STORED_MAX_BYTES  = 512                          # store files smaller than this uncompressed
PNG_QUANTIZE_COLORS   = 256                          # palette size for PNG quantization
PNG_QUANTIZE_MIN_KB   = 20                           # skip quantizing PNGs smaller than this
JPEG_PROGRESSIVE      = True                         # progressive encoding (2–8% smaller)
_PX_PER_EM            = 16.0                         # px per em at standard e-reader baseline
_PT_PER_EM            = 12.0                         # pt per em (12pt = 1em)
LOG_FILE              = 'epubtoolkit.log'            # log file path (relative to cwd)

# ─── Log levels: INFO(1) < NOTE(2) < WARNING(3) < VERBOSE(4) ─────────────────
# Filter: show if level <= _log_level. Default: INFO (Finished/Skipped only).
L_INFO    = 1
L_NOTE    = 2
L_WARNING = 3
L_VERBOSE = 4
LOG_LEVEL_NAMES = {'info': L_INFO, 'note': L_NOTE, 'warning': L_WARNING, 'verbose': L_VERBOSE}

_log_level       = L_INFO
_log_file_handle = None

# ─── Metadata normalisation defaults (all True; see README for details) ────────
NORMALIZE_AUTHORS          = True
DEMOTE_SECONDARY_AUTHORS   = True
NORMALIZE_PUBLISHER        = True
STRIP_PUBLISHER_LEGAL      = True
NORMALIZE_TITLES           = True
CLEAN_TITLE_TAGS           = True
NORMALIZE_SUBJECTS         = True
MAX_SUBJECTS               = 5
CONSOLIDATE_PUBLISHER      = True
CLEAN_PUBLISHER_EXTRA      = True
PRESERVE_GENRE_IMPRINTS    = True   # Tor, Ace, Orbit, Baen, Gollancz kept canonical
PUBLISHER_FUZZY_CORRECT    = False  # opt-in: fuzzy-match publisher typos
ENFORCE_MODIFIED_DATE      = True
ENFORCE_SINGLE_IDENTIFIER  = True
CLEAN_DESCRIPTION_HTML     = True
REMOVE_PAGE_MAP            = True
STRIP_DEPRECATED_HTML_ATTRS = True
STRIP_BODY_BACKGROUND      = True
SCRUB_PRINT_ARTIFACTS      = True
NORMALIZE_MARC_ROLES       = True
REMOVE_FIELDS              = []     # e.g. ["dc:rights"]
STRIP_UNSAFE_CSS           = True
STRIP_JAVASCRIPT           = True

# Extensions that should always be deflate-compressed regardless of size
_ALWAYS_DEFLATE_EXTS = frozenset(['.opf', '.ncx', '.xhtml', '.html', '.htm', '.css'])

BLOAT_FILENAMES   = {
    # OS artifacts
    '.ds_store', 'thumbs.db', 'desktop.ini', '.gitignore',
    # Tool artifacts
    'book.log', 'sigil.cfg', 'page-map.xml',
    # iTunes artifacts
    'itunesmetadata.plist', 'itunesartwork',
    # Retailer noise — DRM-free distributors (Baen, Tor, Smashwords, Kobo, B&N)
    'about-this-ebook.xhtml', 'about-this-ebook.html',
    'rights.xhtml', 'rights.html', 'rights.txt',
    'colophon.txt',
    'smashwords-license-notes.xhtml', 'smashwords-license-notes.html',
    'baen-notice.xhtml', 'baen-notice.html',
}
BLOAT_EXTENSIONS  = {
    '.xpgt',   # Adobe page-template files — dead format
    '.bak',    # backup files left by editing tools
    '.orig',   # original-file backups
}
BLOAT_DIRNAMES    = {'__macosx', '.git', '__pycache__'}
VENDOR_NS_PREFIXES = ('calibre:', 'adobe:', 'ibooks:', 'kobo:', 'sigil:')
MINIFY_EXTENSIONS = ('.html', '.xhtml', '.htm', '.css', '.svg', '.xml', '.opf', '.ncx')
IMAGE_EXTENSIONS  = ('.jpg', '.jpeg', '.png', '.webp')  # WebP added in v2
FONT_EXTENSIONS   = ('.ttf', '.otf', '.woff', '.woff2')
EMPTY_SAFE_TAGS   = {'span', 'div', 'p', 'b', 'i', 'em', 'strong', 'small',
                     'sub', 'sup', 'label', 'li', 'td', 'th'}
CSS_SKIP_TOKENS   = {
    'and', 'not', 'nth', 'child', 'type', 'first', 'last',
    'hover', 'focus', 'active', 'visited', 'before', 'after',
    'root', 'html', 'body', 'span', 'div', 'link', 'of',
}

WIN1252_MAP = {
    '\x80': '\u20ac', '\x82': '\u201a', '\x83': '\u0192', '\x84': '\u201e',
    '\x85': '\u2026', '\x86': '\u2020', '\x87': '\u2021', '\x88': '\u02c6',
    '\x89': '\u2030', '\x8a': '\u0160', '\x8b': '\u2039', '\x8c': '\u0152',
    '\x8e': '\u017d', '\x91': '\u2018', '\x92': '\u2019', '\x93': '\u201c',
    '\x94': '\u201d', '\x95': '\u2022', '\x96': '\u2013', '\x97': '\u2014',
    '\x98': '\u02dc', '\x99': '\u2122', '\x9a': '\u0161', '\x9b': '\u203a',
    '\x9c': '\u0153', '\x9e': '\u017e', '\x9f': '\u0178',
}
# Single-pass replacement regex — avoids O(n × m) repeated str.replace calls
_WIN1252_RE = re.compile('|'.join(re.escape(k) for k in WIN1252_MAP))

# ─── XML Namespaces ───────────────────────────────────────────────────────────
_NS = {
    "opf":     "http://www.idpf.org/2007/opf",
    "dc":      "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "xsi":     "http://www.w3.org/2001/XMLSchema-instance",
    "epub":    "http://www.idpf.org/2007/ops",
}
for _prefix, _uri in _NS.items(): ET.register_namespace(_prefix, _uri)
ET.register_namespace("", _NS["opf"])

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL DATA STRUCTURES & COMPILED REGEXES
# ═══════════════════════════════════════════════════════════════════════════════

_NAME_PARTICLES = frozenset([
    'von', 'van', 'de', 'del', 'della', 'di', 'du', 'la', 'le',
    'los', 'las', 'das', 'des', 'der', 'den', 'al', 'el', 'bin', 'binti',
])

_NAME_SUFFIXES = frozenset([
    'jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv', 'v', 'phd', 'md',
    'esq', 'esq.', 'cbe', 'obe', 'mbe',
])

# Academic/professional credentials that should be stripped from display names.
# These appear after a comma and cause the inversion detector to misfire,
# treating the credential as the surname.
# e.g. "David R. Hawkins, M.D., Ph.D." → "David R. Hawkins"
#      "Andrew Weil MD"                 → "Andrew Weil"
_AUTHOR_CREDENTIAL_RE = re.compile(
    r'(?:,\s*|\s+)(?:M\.?D\.?|Ph\.?[Dd]\.?|Phd|D\.?D\.?S\.?|D\.?M\.?D\.?|'
    r'R\.?N\.?|M\.?B\.?A\.?|J\.?D\.?|Ed\.?D\.?|Psy\.?D\.?|'
    r'D\.?O\.?|D\.?V\.?M\.?|Pharm\.?D\.?)\s*$',
    re.IGNORECASE,
)

# Editor role annotations that belong in dc:contributor, not in the display name.
# e.g. "Ann Charters (ed.)" → "Ann Charters"   file_as="Charters, Ann"
# Stripped silently; the book's OPF role attribute (if present) carries the info.
_AUTHOR_EDITOR_RE = re.compile(
    r'\s*\(\s*(?:ed|eds|editor|editors)\.?\s*\)\s*$',
    re.IGNORECASE,
)

_PUBLISHER_LEGAL_SUFFIXES = re.compile(
    r',?\s*(?:Incorporated|Limited|Pty\.?\s*Limited|Pty\.?\s*Ltd\.?|'
    r'Inc\.?|LLC\.?|Ltd\.?|L\.L\.C\.?|Corp\.?|Co\.?|GmbH|S\.A\.?|B\.V\.?|ULC\.?)$',
    re.IGNORECASE,
)

# Short words that should stay lowercase in title-cased publisher names
_PUB_LOWERCASE_WORDS = frozenset([
    'a', 'an', 'and', 'at', 'but', 'by', 'for', 'in', 'nor',
    'of', 'on', 'or', 'so', 'the', 'to', 'up', 'yet',
])

# Strip URLs from any text field
_URL_RE = re.compile(r'https?://\S+|www\.\S+', re.IGNORECASE)

# Remove trailing parentheticals, "& Co.", and verbose business suffixes
_PUB_PAREN_RE  = re.compile(r'\s*\([^)]*\)', re.IGNORECASE)

_PUB_AND_CO_RE = re.compile(r'\s+&\s+Co\.?', re.IGNORECASE)

_PUB_SUFFIX_RE = re.compile(
    r'\s*[,\-]?\s*(?:Group|Publishing|Publishers|Books|Press|Media|Entertainment|'
    r'International|Digital|Global|Worldwide|Editions?|House|Studio|Network|'
    r'Productions?|Classics?|Library|Publishing\s+Group|Publishing\s+House)\s*$',
    re.IGNORECASE,
)

# City-prefixed bibliographic entries: "New York : Tor, 2008."  →  "Tor"
_CITY_PREFIX_RE = re.compile(
    r'^(?:New\s+York|London|Boston|Chicago|San\s+Francisco|Richmond|'
    r'Oxford|Cambridge|Toronto|Sydney|Melbourne|Edinburgh)\s*:\s*'
    r'([^,\d][^,]*?)(?:,\s*(?:c?\d{4}|\[).*)?$',
    re.IGNORECASE,
)

# Garbage detection: values that should be blanked entirely
_PUBLISHER_GARBAGE_RE = re.compile(
    r'var\s+newTip|wtrButtonContainer|<div\s+class=|'   # Goodreads JS blobs
    r'archiveofourown\.org|fanfiction\.net|'             # fanfic sites
    r'royalroad\.com|spacebattles\.com|sufficientvelocity\.com|'
    r'questionablequesting\.com|tthfanfic\.org|'
    r'forum\.|forums\.',
    re.IGNORECASE,
)

# Known "and" → "&" normalisations (only exact known publisher names)
_PUB_AMP_NORM = [
    (re.compile(r'\bSimon\s+and\s+Schuster\b',   re.I), 'Simon & Schuster'),
    (re.compile(r'\bFaber\s+and\s+Faber\b',      re.I), 'Faber & Faber'),
    (re.compile(r'\bJohn\s+Wiley\s+and\s+Sons\b', re.I), 'John Wiley & Sons'),
    (re.compile(r'\bHodder\s+and\s+Stoughton\b',  re.I), 'Hodder & Stoughton'),
    (re.compile(r'\bConstable\s+and\s+Robinson\b', re.I), 'Constable & Robinson'),
]

# Strip mid-string "Pty" (Australian entity word) before legal suffix strip
_PUB_PTY_RE = re.compile(r',?\s*\bPty\.?\b,?\s*', re.IGNORECASE)

# Typo / misspelling corrections for known publisher name errors
_PUBLISHER_TYPO_MAP = [
    # Misspellings caught in real library data
    (re.compile(r'\bHoughton\s+Miffin\b',      re.I), 'Houghton Mifflin'),
    (re.compile(r'\bHoughton\s+Miflin\b',      re.I), 'Houghton Mifflin'),
    (re.compile(r'\bHarper\s+Collins\b',       re.I), 'HarperCollins'),
    (re.compile(r'\bHarpr\s*Collins\b',        re.I), 'HarperCollins'),
    (re.compile(r'\bMacMillan\b'),                     'Macmillan'),
    (re.compile(r'\bMacmillian\b',             re.I), 'Macmillan'),
    (re.compile(r"\bSt\.?\s*Martin'?s?s\b",   re.I), "St. Martin's"),
    (re.compile(r'\bPenguine\b',               re.I), 'Penguin'),
    (re.compile(r'\bRandom\s+Hous\b',         re.I), 'Random House'),
    (re.compile(r'\bScribener\b',              re.I), 'Scribner'),
    (re.compile(r'\bDoubelday\b',              re.I), 'Doubleday'),
    (re.compile(r'\bAnchor\s+Book\b',         re.I), 'Anchor Books'),
    (re.compile(r'\bHoltzbrinck\b',            re.I), 'Holtzbrinck'),   # normalize variant
    (re.compile(r'\bFarrar\s*,?\s*Strauss\b',  re.I), 'Farrar, Straus'),
    (re.compile(r'\bBallatine\b',              re.I), 'Ballantine'),
    (re.compile(r'\bBantum\b',                re.I), 'Bantam'),
    (re.compile(r'\bKnoph\b',                 re.I), 'Knopf'),
]

# Imprint → parent publisher consolidation map.
# Harlequin block runs first: it is now a HarperCollins imprint but the brand
# is dominant enough to keep as its own canonical name rather than fold into HC.
# Imprint → parent publisher consolidation map.
# Stored as a plain dict for easy maintenance; compiled to regex tuples at import time.
# To add a new publisher: add an entry here, no regex knowledge needed.
# Harlequin runs first: it is now a HarperCollins imprint but the brand is dominant
# enough in romance to keep as its own canonical name.
_IMPRINT_MAP_RAW: dict[str, list[str]] = {
    'Harlequin': ['Harlequin'],  # matches any Harlequin variation
    'Penguin Random House': [
        'Viking', 'G. P. Putnam', 'G.P. Putnam', 'Avery', 'Knopf',
        'Alfred A. Knopf', 'Doubleday', 'Vintage', 'Anchor', 'Ballantine',
        'Bantam', 'Crown', 'Delacorte', 'Dell', 'Dial Press', 'Dutton',
        'Riverhead', 'Plume', 'Signet', 'NAL', 'New American Library',
        'Berkley', 'Ace', 'Ace Books', 'Jove', 'Roc', 'Putnam', 'Gotham',
        'Blue Rider', 'Portfolio', 'Sentinel', 'Random House', 'Penguin',
        'Del Rey', 'DAW', 'DAW Books', 'Spectra', 'WaterBrook', 'Ten Speed',
        'Transworld', 'Broadway', 'Broadway Books',
    ],
    'HarperCollins': [
        'William Morrow', 'Avon', 'Zondervan', 'Thomas Nelson',
        'Amistad', 'Ecco', 'Harper Perennial', 'Harper Voyager', 'Voyager',
        'HarperCollins', 'Harper Collins', 'HarperTorch', 'HarperTrophy',
        'HarperPrism', 'HarperPress', 'Harper Morrow', 'HarperElement',
        'Custom House', 'Fourth Estate', 'Witness Impulse', 'It Books',
    ],
    'Simon & Schuster': [
        'Scribner', 'Atria', 'Touchstone', 'Pocket Books', 'Gallery Books',
        'Free Press', 'Howard Books', 'Adams Media', 'Simon Spotlight',
        'Avid Reader', 'Simon Pulse', 'Saga Press',
        'Margaret K. McElderry', 'Simon & Schuster', 'Simon and Schuster',
    ],
    'Hachette': [
        'Little Brown', 'Orbit', 'Grand Central', 'Mulholland',
        'Yen Press', 'Basic Books', 'Twelve', 'Gollancz', 'Hachette',
        'Balance', 'Forever', 'FaithWords', 'Center Street',
        'Running Press', 'Da Capo',
    ],
    'Macmillan': [
        'Pan Macmillan', 'Macmillan', 'Tor', 'Tordotcom', 'Tor.com',
        'Orb', 'Orb Books', 'Forge', 'Minotaur', 'St. Martin',
        "St. Martin's", 'Henry Holt', 'Picador', 'Farrar Straus',
        'Flatiron', 'First Second', 'Roaring Brook', 'Square Fish',
        'Tom Doherty',
    ],
    'Baen': ['Baen', 'Baen Books', 'Baen Fantasy', 'Baen Publishing Enterprises'],
    'Kensington': [
        'Kensington', 'Zebra', 'Zebra Books', 'Pinnacle', 'Pinnacle Books',
        'Lyrical Press',
    ],
    'Scholastic': ['Scholastic', 'Scholastic Press', 'Arthur A. Levine', 'Chicken House'],
    'Amazon Publishing': [
        '47North', 'Thomas & Mercer', 'Lake Union', 'Montlake', 'Montlake Romance',
        'Amazon Crossing', 'Skyscape', 'Kindle Direct Publishing', 'KDP',
        'Amazon Digital Services', 'Amazon Publishing', 'Amazon Studios',
    ],
    'Disney Publishing': [
        'Disney-Hyperion', 'Disney Hyperion', 'Hyperion', 'Hyperion Books',
        'Disney Press', 'Disney Editions', 'Disney Publishing',
        'Lucasfilm Press', 'Marvel Press', 'Marvel Comics', 'Marvel Entertainment',
    ],
    'Bloomsbury': [
        'Bloomsbury', 'Bloomsbury Publishing', 'Bloomsbury Academic',
        "Bloomsbury Children's Books", 'Bloomsbury USA', 'Bloomsbury UK',
    ],
    'Quarto Group': [
        'Quarto', 'Quarto Publishing Group', 'Frances Lincoln',
        'Frances Lincoln Adult', 'Fair Winds Press', 'Chartwell Books',
        'Rock Point', 'Cool Springs Press', 'Walter Foster', 'Motorbooks',
    ],
    'VIZ Media': ['VIZ Media', 'Viz', 'Shonen Jump', 'Shojo Beat', 'VIZ Signature'],
    'Kodansha': ['Kodansha', 'Kodansha Comics', 'Kodansha USA', 'Vertical', 'Vertical Comics'],
    'Dark Horse': ['Dark Horse', 'Dark Horse Comics', 'Dark Horse Books', 'Dark Horse Manga'],
    'Oxford University Press': ['Oxford University Press', 'OUP Oxford', 'OUP USA'],
    'Cambridge University Press': ['Cambridge University Press', 'CUP'],
    'National Geographic': [
        'National Geographic', 'National Geographic Books',
        'National Geographic Society', 'National Geographic Partners',
    ],
    'Thames & Hudson': ['Thames & Hudson', 'Thames and Hudson'],
    'Tachyon Publications': ['Tachyon', 'Tachyon Publications'],
    'Subterranean Press': ['Subterranean', 'Subterranean Press'],
    'Night Shade Books': ['Night Shade', 'Night Shade Books'],
    'Skybound': ['Skybound', 'Skybound Books', 'Skybound Entertainment'],
    'Angry Robot': ['Angry Robot', 'Angry Robot Books'],
    'Rebellion Publishing': ['Rebellion', 'Rebellion Publishing', 'Solaris', 'Solaris Books', 'Abaddon Books'],
    'Smashwords': ['Smashwords', 'Smashwords Inc.'],
    'Draft2Digital': ['Draft2Digital', 'Draft 2 Digital'],
    'IngramSpark': ['IngramSpark', 'Ingram Spark'],
}

def _compile_imprint_map(raw: dict) -> list:

    result = []
    for canonical, aliases in raw.items():
        names = list(dict.fromkeys(aliases))  # dedup while preserving order
        escaped = [re.escape(a) for a in names if a]
        if not escaped: continue
        pattern = re.compile(r'\b(?:' + '|'.join(escaped) + r')\b', re.IGNORECASE)
        result.append((pattern, canonical))
    return result

_IMPRINT_MAP = _compile_imprint_map(_IMPRINT_MAP_RAW)

# Imprints that are genre-defining brands — readers search by these names and
# they should NOT be collapsed to their corporate parent even when
# CONSOLIDATE_PUBLISHER / --consolidate-publisher is on.
# Applies only when PRESERVE_GENRE_IMPRINTS = True.
_GENRE_DEFINING_IMPRINTS = frozenset([
    # SFF powerhouses with strong reader brand identity
    'Tor', 'Baen', 'Ace', 'Orbit', 'Gollancz', 'DAW', 'Del Rey',
    'Tachyon Publications', 'Subterranean Press', 'Night Shade Books',
    'Skybound', 'Angry Robot', 'Rebellion Publishing',
    # Romance
    'Harlequin',
    # Comics / manga
    'VIZ Media', 'Kodansha', 'Dark Horse',
    # Literary independents
    'Graywolf Press', 'Archipelago Books', 'Coffee House Press',
    'Two Dollar Radio', 'Restless Books', 'Soho Press', 'Melville House',
])


# Covers English, French, German, Spanish, Italian, Portuguese, Dutch,
# Swedish, Norwegian, Danish, and a handful of other common library languages.
_TITLE_ARTICLES = frozenset([
    # English
    'the', 'a', 'an',
    # French
    'le', 'la', 'les', "l'", 'un', 'une', 'des',
    # German
    'der', 'die', 'das', 'ein', 'eine',
    # Spanish
    'el', 'los', 'las', 'un', 'una', 'unos', 'unas',
    # Italian
    'il', 'lo', 'gli', 'uno',
    # Portuguese
    'o', 'os', 'as',
    # Dutch
    'de', 'het', 'een',
    # Swedish / Norwegian / Danish
    'en', 'ett', 'den', 'det',
])

# Title-case word lists: words that stay lowercase inside a title
# (only when not the first word and not after a colon/em-dash).
_TITLE_LOWERCASE_WORDS = frozenset([
    'a', 'an', 'and', 'as', 'at', 'but', 'by', 'for',
    'if', 'in', 'nor', 'of', 'on', 'or', 'so', 'the',
    'to', 'up', 'via', 'yet',
])

# ── Title cleanup regexes ────────────────────────────────────────────────────
_TITLE_TRAILING_PAREN_RE   = re.compile(r'\s*\([^)]*\)\s*$')

_TITLE_TRAILING_BRACKET_RE = re.compile(r'\s*\[[^\]]*\]\s*$')

# Dash form:  "Title — A Novel", "Title - A Thriller"  (already existed)
_TITLE_GENRE_SUFFIX_RE     = re.compile(
    r'\s*[-—–]\s*(?:A|An)\s+'
    r'(?:Novel|Thriller|Mystery|Memoir|Romance|Fantasy\s+Novel|'
    r'Historical\s+Novel|Short\s+Story|Novella|Collection|Story|'
    r'True\s+Story|Biography|Narrative)\s*$',
    re.IGNORECASE,
)

# Colon form: "Title: A Novel", "Title: A Memoir", "Title: Stories"
# 1,934 entries in real library data.  The colon is kept when the subtitle
# is substantive (e.g. "Dune: Messiah") — we only strip the subset of
# generic form-descriptors that add no information.
_TITLE_COLON_GENRE_RE      = re.compile(
    r':\s*(?:A|An)\s+'
    r'(?:Novel|Thriller|Mystery|Memoir|Romance|Fantasy\s+Novel|'
    r'Historical\s+Novel|Short\s+Story|Novella|Collection|Story|'
    r'True\s+Story|Biography|Narrative|Tale|Epic)\s*$',
    re.IGNORECASE,
)

# Sort-form inversion: "Shadow Rising, The" → display "The Shadow Rising"
# 12 titles in real library data.
_TITLE_THE_INVERSION_RE    = re.compile(r'^(.+),\s+(the)\s*$', re.IGNORECASE)

_SUBJECT_BLOCKLIST = frozenset([
    # Format / DRM noise
    'ebook', 'ebooks', 'e-book', 'e-books', 'epub', 'mobi', 'kindle',
    'kindleunlimited', 'kindle unlimited', 'ku',
    'digital', 'retail', 'drm', 'drm-free', 'pdf', 'audio', 'audiobook',
    'audiobooks', 'audio book', 'abridged', 'unabridged', 'illustrated',
    'large print', 'electronic books', 'electronic book',
    # Uselessly broad genre/form
    'fiction', 'general', 'nonfiction', 'non-fiction',
    'book', 'books', 'reading', 'literature', 'novel', 'novella',
    # Popularity noise
    'bestseller', 'bestselling', 'popular',
    # Unknown/placeholder
    'unknown', 'other', 'miscellaneous', 'uncategorized', 'uncategorised',
    'undefined', 'null', 'none',
    # Personal shelving tags that sometimes leak into subject fields
    'to-read', 'tbr', 'dnf', 'owned', 'wishlist',
])

# BISAC-style machine codes that appear as subject tags: BIO000000, BUS027000,
# sci_biology, sf_fantasy, etc.
_SUBJECT_BISAC_RE = re.compile(r'^[A-Z]{2,4}\d{6}$|^(?:sci|sf|bio|bus)_\w+$', re.I)

# Library "—Fiction" suffix: "Amish—Fiction", "Vampires--Fiction"
# Strip the suffix and keep the topic word.
# Guard: don't strip from "Non-Fiction", "Science-Fiction", "Fan-Fiction".
_SUBJECT_FICTION_SUFFIX_RE = re.compile(r'\s*[-–—]+\s*fiction\s*$', re.I)

_SUBJECT_FICTION_WHOLE = re.compile(r'^(?:non-?fiction|science-?fiction|fan-?fiction)$', re.I)

# Typo corrections for subjects found in real library data
_SUBJECT_TYPO_MAP = {
    'apocalpyse':          'Apocalypse',
    'assissination':       'Assassination',
    'authobiography':      'Autobiography',
    'postapocalypitic':    'Post-Apocalyptic',
    'postapocalypse':      'Post-Apocalyptic',
    'postapcalyptic':      'Post-Apocalyptic',
    'psyschology':         'Psychology',
    'stratagies':          'Strategies',
    'resillience':         'Resilience',
    'reconcilliation':     'Reconciliation',
    'syncrhonicity':       'Synchronicity',
    'syncronicity':        'Synchronicity',
}

# ── MARC relator codes ────────────────────────────────────────────────────────
_MARC_ROLE_MAP = {
    'author': 'aut', 'writer': 'aut',
    'editor': 'edt', 'edited by': 'edt',
    'translator': 'trl', 'translated by': 'trl',
    'illustrator': 'ill', 'illus': 'ill',
    'introduction': 'aui', 'foreword': 'aui', 'preface': 'aui',
    'narrator': 'nrt',
    'photographer': 'pht',
    'compiler': 'com',
    'contributor': 'ctb',
    'cover artist': 'cov', 'cover design': 'cov',
}

# ── dc:description HTML stripping ────────────────────────────────────────────
_DESC_HTML_TAG_RE = re.compile(r'<[^>]+>')

_DESC_ENTITY_RE   = re.compile(r'&(?:[a-zA-Z]+|#\d+|#x[0-9a-fA-F]+);')

_HTML_ENTITIES = {
    '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"',
    '&apos;': "'", '&nbsp;': ' ', '&mdash;': '\u2014',
    '&ndash;': '\u2013', '&hellip;': '\u2026', '&ldquo;': '\u201c',
    '&rdquo;': '\u201d', '&lsquo;': '\u2018', '&rsquo;': '\u2019',
}
# Single-pass entity replacement regex — avoids O(n × m) repeated str.replace calls
_HTML_ENTITIES_RE = re.compile('|'.join(re.escape(k) for k in _HTML_ENTITIES))

# ── Print artifact scrubbing ─────────────────────────────────────────────────
_PRINT_ARTIFACT_RES = [
    # Printer's key: "10 9 8 7 6 5 4 3 2 1" — run of space-separated numbers
    re.compile(r'<p[^>]*>\s*(?:\d+\s+){3,}\d+\s*</p>', re.IGNORECASE),
    re.compile(r'<p[^>]*>[^<]*printed\s+(?:on\s+acid[- ]free|in\s+the\s+United\s+States)[^<]*</p>',
               re.IGNORECASE),
    re.compile(r'<p[^>]*>[^<]*first\s+published\s+in\s+[^<]{3,60}</p>', re.IGNORECASE),
    re.compile(r'<p[^>]*>[^<]*manufactured\s+in\s+the\s+[^<]{2,30}</p>', re.IGNORECASE),
]

# ── Deprecated HTML attribute removal ────────────────────────────────────────
_DEPRECATED_ATTRS_RE = re.compile(
    r"""\s+(?:align|bgcolor|border|valign|cellspacing|cellpadding|hspace|vspace)"""
    r"""\s*=\s*(?:"[^"]*"|'[^']*'|\S+)""",
    re.IGNORECASE,
)

# ── Body background-color stripping ──────────────────────────────────────────
# Matches body/html rules at the top selector level only.
# Uses a word-boundary assertion to avoid matching "tbody", "anybody", etc.
# The negative lookbehind (?<![a-z]) prevents matching class/id selectors ending in "body".
_BODY_BG_COLOR_RE = re.compile(
    # Matches background[-color] but not background-image or background:url(…)
    r'((?:^|\})\s*(?:[^{]*\s)?(?<![a-z])(?:body|html)(?![a-z-])[^{]*\{[^}]*?)'
    r'background(?!-image)(?:-color)?\s*:(?![^;]*url\()[^;]+;',
    re.IGNORECASE | re.MULTILINE,
)

# Pre-compiled splitter: splits HTML into alternating [text, tag, text, tag …]
# Even indices → text nodes (safe to normalise); odd indices → raw tag strings.
_TAG_SPLIT_RE = re.compile(r'(<[^>]+>)')

# ── Invisible / problematic Unicode characters ─────────────────────────────
# Injected by Word, InDesign, and old converters.  Invisible to readers but
# break text search, copy-paste, TTS read-aloud, and automatic hyphenation.
_INVISIBLE_CHARS_RE = re.compile(
    '['
    '\u00AD'   # SOFT HYPHEN (&shy;) — pre-placed hyphenation hint
    '\u200B'   # ZERO WIDTH SPACE
    '\u200C'   # ZERO WIDTH NON-JOINER
    '\u200D'   # ZERO WIDTH JOINER
    '\u2060'   # WORD JOINER
    '\uFEFF'   # BOM / ZERO WIDTH NO-BREAK SPACE when appearing mid-text
    '\u200E'   # LEFT-TO-RIGHT MARK
    '\u200F'   # RIGHT-TO-LEFT MARK
    ']',
    re.UNICODE
)

# Non-breaking space chains used as visual paragraph indentation.
# &#160; and \u00A0 are the numeric/unicode forms of &nbsp;.
# We only replace runs of 2+ so a single nbsp (e.g. "Dr.\u00A0Smith") survives.
_NBSP_INDENT_RE = re.compile(r'(?:\u00A0|&#160;|&nbsp;){2,}')

# Inline style property values that are identical to browser/reader defaults
# and carry no meaningful formatting — safe to strip unconditionally.
# Keys are lowercase property names; values are sets of lowercase no-op values.
_UNIVERSAL_JUNK = frozenset({'initial', 'inherit'})
_ZERO_JUNK      = frozenset({'0', '0px', '0em', '0pt'}) | _UNIVERSAL_JUNK
_JUNK_INLINE_PROPS = {
    'color':            {'inherit', 'initial', 'unset'},
    'background':       {'transparent', 'none'} | _UNIVERSAL_JUNK,
    'background-color': {'transparent'} | _UNIVERSAL_JUNK,
    'font-style':       {'normal'} | _UNIVERSAL_JUNK,
    'font-weight':      {'normal', '400'} | _UNIVERSAL_JUNK,
    'font-variant':     {'normal'} | _UNIVERSAL_JUNK,
    'text-decoration':  {'none'} | _UNIVERSAL_JUNK,
    'text-transform':   {'none'} | _UNIVERSAL_JUNK,
    'text-indent':      _ZERO_JUNK,
    'margin':           _ZERO_JUNK,
    'margin-top':       _ZERO_JUNK,
    'margin-bottom':    _ZERO_JUNK,
    'margin-left':      _ZERO_JUNK,
    'margin-right':     _ZERO_JUNK,
    'padding':          _ZERO_JUNK,
    'padding-top':      _ZERO_JUNK,
    'padding-bottom':   _ZERO_JUNK,
    'padding-left':     _ZERO_JUNK,
    'padding-right':    _ZERO_JUNK,
    'border':           {'none', '0'} | _UNIVERSAL_JUNK,
    'vertical-align':   {'baseline'} | _UNIVERSAL_JUNK,
    'display':          _UNIVERSAL_JUNK,
    'visibility':       {'visible'} | _UNIVERSAL_JUNK,
    'overflow':         {'visible'} | _UNIVERSAL_JUNK,
    'line-height':      {'normal'} | _UNIVERSAL_JUNK,
    'letter-spacing':   {'normal', '0', '0em'} | _UNIVERSAL_JUNK,
    'word-spacing':     {'normal', '0', '0em'} | _UNIVERSAL_JUNK,
}

_VOID_ELEMENTS = frozenset([
    'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input',
    'link', 'meta', 'param', 'source', 'track', 'wbr',
])

# Common named entities not in XML base set → numeric equivalents
_ENTITY_MAP = {
    '&nbsp;': '&#160;', '&mdash;': '&#8212;', '&ndash;': '&#8211;',
    '&hellip;': '&#8230;', '&ldquo;': '&#8220;', '&rdquo;': '&#8221;',
    '&lsquo;': '&#8216;', '&rsquo;': '&#8217;', '&copy;': '&#169;',
    '&reg;': '&#174;', '&trade;': '&#8482;', '&bull;': '&#8226;',
    '&middot;': '&#183;', '&laquo;': '&#171;', '&raquo;': '&#187;',
    '&apos;': '&apos;',  # already valid XML
}

# Pattern to detect named HTML entities not declared in the DTD
_NAMED_ENTITY_RE = re.compile(r'&([a-zA-Z][a-zA-Z0-9]*);')

# Void element tags that are not self-closed: <br>, <br >, <br class="…">
_VOID_TAG_RE = re.compile(
    r'<(' + '|'.join(_VOID_ELEMENTS) + r')(\s[^>]*)?>(?!\s*/)',
    re.IGNORECASE)

_SCRIPT_TAG_RE      = re.compile(r'<script\b[^>]*>.*?</script>', re.IGNORECASE | re.DOTALL)

_SCRIPT_SELF_CLOSE  = re.compile(r'<script\b[^>]*/>', re.IGNORECASE)

_INLINE_HANDLER_RE  = re.compile(
    r'\s+on[a-zA-Z]+\s*=\s*(?:"[^"]*"|\'[^\']*\')', re.IGNORECASE)

_UNSAFE_PROPS_BASE = frozenset(['position'])         # only 'fixed' value is removed

_UNSAFE_UNITS_RE   = re.compile(r':\s*[^;{}]*\b\d+(?:\.\d+)?\s*v[hw]\b', re.IGNORECASE)

_FIXED_VAL_RE      = re.compile(r'\bposition\s*:\s*fixed\b', re.IGNORECASE)

_ABSOLUTE_VAL_RE   = re.compile(r'\bposition\s*:\s*absolute\b', re.IGNORECASE)

_FLOAT_PROP_RE     = re.compile(r'\bfloat\s*:\s*(?!none)[^;]+;?', re.IGNORECASE)

# Declaration-level regex: match a single CSS declaration ending in ; or }
_DECL_RE = re.compile(r'([a-zA-Z-]+)\s*:\s*([^;{}]+)\s*;?')

_ADDRESS_OPEN_RE  = re.compile(r'<address(\b[^>]*)>', re.IGNORECASE)

_ADDRESS_CLOSE_RE = re.compile(r'</address>', re.IGNORECASE)

# Editor namespaces that are purely authoring metadata, never rendered
_SVG_EDITOR_NS = (
    'sodipodi:', 'inkscape:', 'dc:', 'cc:', 'rdf:',
    'ai:', 'xap:', 'xmpMM:', 'stRef:', 'pdfx:',
    'illustrator:',
)

_SVG_EDITOR_NS_DECL_RE = re.compile(
    r'\s+xmlns:(?:sodipodi|inkscape|dc|cc|rdf|ai|xap|xmpMM|stRef|pdfx|illustrator)'
    r'="[^"]*"',
    re.IGNORECASE)

# Remove entire editor-namespace elements
_SVG_EDITOR_ELEM_RE = re.compile(
    r'<(?:sodipodi|inkscape|rdf:RDF|cc:|dc:|xmp:)[^>]*/?>|'
    r'<(?:sodipodi|inkscape|rdf:RDF|cc:|dc:|xmp:)[^>]*>.*?</[^>]+>',
    re.IGNORECASE | re.DOTALL)

# Remove XML comments inside SVG
_SVG_COMMENT_RE = re.compile(r'<!--.*?-->', re.DOTALL)

# Collapse excessive whitespace in path data: d=" M 10 20 L 30 40 " → d="M10 20L30 40"
_PATH_DATA_RE   = re.compile(r'(\bd=")(\s*[^"]+)(")', re.IGNORECASE)

# EPUB2 OPF <guide> valid reference types
_GUIDE_VALID_TYPES = frozenset([
    'cover', 'title-page', 'toc', 'index', 'glossary', 'acknowledgements',
    'bibliography', 'colophon', 'copyright-page', 'dedication', 'epigraph',
    'foreword', 'loi', 'lot', 'notes', 'preface', 'text',
])

# Properties to convert (only font/text sizing — NOT layout widths/margins)
_FONT_SIZE_PROPS = frozenset([
    'font-size', 'line-height', 'letter-spacing', 'word-spacing',
])

_FONT_SIZE_DECL_RE = re.compile(
    r'(\b(?:font-size|line-height|letter-spacing|word-spacing)\s*:\s*)'
    r'(-?[\d.]+)(px|pt)\b',
    re.IGNORECASE
)

_EMPTY_BODY_RE = re.compile(
    r'<body\b[^>]*>\s*((?:<(?:br|p|div|span)\b[^>]*/?>|\s)*)\s*</body>',
    re.IGNORECASE | re.DOTALL
)

_MEANINGFUL_TEXT_RE = re.compile(r'[^\s<>]')   # any non-whitespace outside tags

# Words that are too generic to be a meaningful series name on their own.
# Stripping "Series" from these produces noise, so we leave them intact.
_SERIES_GENERIC_WORDS = frozenset([
    'adventure', 'anthology', 'collection', 'stories', 'story',
    'trilogy', 'saga', 'universe', 'chronicles', 'series',
    'tales', 'novels', 'books',
])

# Placeholder values that mean "this book has no series" — blank these.
_SERIES_PLACEHOLDER_RE = re.compile(
    r'^(?:000-no\s+series|stand.?alone|novel|none|n/?a|-+|\?+|no\s+series)$',
    re.IGNORECASE,
)

# Reading/publication-order parentheticals to strip:
# "Darkover (Publication Order)" → "Darkover"
# "Foundation (Chronological Order)" → "Foundation"
_SERIES_ORDER_PAREN_RE = re.compile(
    r',?\s*\(\s*(?:Publication|Chronolog\w*|Reading)\s+Order\s*\)\s*$',
    re.IGNORECASE,
)

# Inline reading-order suffixes after colon or dash:
# "Hornblower Saga: Chronological Order" → "Hornblower Saga"
# "Shannara Universe: Chronological"     → "Shannara Universe"
_SERIES_ORDER_SUFFIX_RE = re.compile(
    r'[:\s\-]+(?:Chronolog\w*(?:\s+Order)?|Publication\s+Order|Reading\s+Order)\s*$',
    re.IGNORECASE,
)

# "X Series" suffix — only strip when the stem is still meaningful
_SERIES_SUFFIX_RE = re.compile(r',?\s+[Ss]eries$')

_EMPTY_SPAN_RE = re.compile(
    r'<(span|div)\b([^>]*)>\s*</\1>',
    re.IGNORECASE
)

# Captures img attributes and optional trailing self-close slash separately.
# The trailing / is not included in group(1) to prevent broken attr injection.
_IMG_TAG_RE = re.compile(r'<img\b([^>]*?)(\s*/?)>', re.IGNORECASE | re.DOTALL)

# HTML block elements that are forbidden inside <p> per the HTML spec
_BLOCK_ELEMENTS = frozenset([
    'address', 'article', 'aside', 'blockquote', 'canvas', 'dd', 'details',
    'dialog', 'div', 'dl', 'dt', 'fieldset', 'figcaption', 'figure',
    'footer', 'form', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'header',
    'hgroup', 'hr', 'li', 'main', 'nav', 'noscript', 'ol', 'p', 'pre',
    'section', 'summary', 'table', 'ul',
])

_OPEN_BLOCK_IN_P_RE = re.compile(
    r'<p\b[^>]*>(?:(?!</p>).)*?<('
    + '|'.join(sorted(_BLOCK_ELEMENTS))
    + r')\b',
    re.IGNORECASE | re.DOTALL
)

# Includes GIF — static GIFs are converted to PNG
IMAGE_EXTENSIONS_ALL  = ('.jpg', '.jpeg', '.png', '.webp', '.gif')
IMAGE_EXTENSIONS_WITH_SVG = IMAGE_EXTENSIONS_ALL + ('.svg',)

_DATA_URI_RE = re.compile(
    r'data:(image/(?:jpeg|jpg|png|gif|webp|svg\+xml));base64,([A-Za-z0-9+/=\s]+)',
    re.IGNORECASE
)

_MIME_TO_EXT = {
    'image/jpeg': '.jpg', 'image/jpg': '.jpg',
    'image/png':  '.png', 'image/gif': '.gif',
    'image/webp': '.webp', 'image/svg+xml': '.svg',
}



# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class DRMProtectedError(Exception): pass

class StructuralValidationError(Exception): pass

# ── Metadata options bundle ───────────────────────────────────────────────────
from dataclasses import dataclass, field as _dc_field

@dataclass
class MetaOpts:
    
    normalize_authors:         bool = True
    demote_secondary_authors:  bool = True
    normalize_publisher:       bool = True
    strip_publisher_legal:     bool = True
    consolidate_publisher:     bool = True
    clean_publisher_extra:     bool = True
    normalize_titles:          bool = True
    clean_title_tags:          bool = True
    normalize_subjects:        bool = True
    max_subjects:              int  = 5
    enforce_modified_date:     bool = True
    enforce_single_identifier: bool = True
    clean_description_html:    bool = True
    normalize_marc_roles:      bool = True
    remove_fields:             list = _dc_field(default_factory=list)

    def as_kwargs(self) -> dict:

        return {
            'normalize_authors':         self.normalize_authors,
            'demote_secondary_authors':  self.demote_secondary_authors,
            'normalize_publisher':       self.normalize_publisher,
            'strip_publisher_legal':     self.strip_publisher_legal,
            'consolidate_publisher':     self.consolidate_publisher,
            'clean_publisher_extra':     self.clean_publisher_extra,
            'normalize_titles':          self.normalize_titles,
            'clean_title_tags':          self.clean_title_tags,
            'normalize_subjects':        self.normalize_subjects,
            'max_subjects':              self.max_subjects,
            'enforce_modified_date':     self.enforce_modified_date,
            'enforce_single_identifier': self.enforce_single_identifier,
            'clean_description_html':    self.clean_description_html,
            'normalize_marc_roles':      self.normalize_marc_roles,
            'remove_fields':             self.remove_fields,
        }


from contextlib import contextmanager as _contextmanager

@_contextmanager
def _step(name: str, path: str = ''):
    # Catches exceptions, calls _warn(), and continues — used to isolate pipeline steps.
    try:
        yield
    except Exception as _e:
        _warn(f"{name} failed", path, _e)

# ── Warning helper ────────────────────────────────────────────────────────────
_warn_log_handle = None    # set by the batch runner to mirror warnings into the log
_warn_count      = 0       # total warnings issued this session (for summary)

def _warn(msg: str, path: str = '', exc: Exception = None) -> None:
    global _warn_count
    _warn_count += 1
    parts = [f"\n  Warning: {msg}"]
    if path: parts.append(f" for {os.path.basename(path)}")
    if exc:  parts.append(f": {exc}")
    line = ''.join(parts)
    print(line, file=sys.stderr)
    if _warn_log_handle is not None:
        try:
            _warn_log_handle.write(
                f"{datetime.now().strftime('%H:%M:%S')}  [WARN]  {line.strip()}\n")
            _warn_log_handle.flush()
        except Exception: pass

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — FILE UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def read_raw(path):
    with open(path, 'rb') as f: return f.read()

def read_text(path):
    raw = read_raw(path)
    if HAS_CHARSET_NORMALIZER:
        result = detect_encoding(raw).best()
        enc = str(result.encoding) if result else 'utf-8'
    else: enc = 'utf-8'
    return raw.decode(enc, errors='replace')

def write_text(path, content):
    with open(path, 'w', encoding='utf-8') as f: f.write(content)

def write_if_changed(path, original, new):

    if new != original:
        write_text(path, new)
        return True
    return False


# ── Per-EPUB file-index cache ─────────────────────────────────────────────────
# Populated once by build_epub_index() right after ZIP extraction.
# All find_*() helpers read from the cache instead of re-walking the tree.
# Call invalidate_epub_index() whenever a pipeline step creates new HTML/OPF
# files on disk (currently: generate_epub3_nav, split_large_xhtml).
# The index is keyed by temp_dir so parallel workers never collide.

_epub_index: dict = {}   # temp_dir → {"opf": [...], "html": [...], "css": [...], "ncx": [], "nav": [], "html_text": {}}

def build_epub_index(temp_dir: str) -> None:
    
    idx: dict = {"opf": [], "html": [], "css": [], "ncx": [], "nav": [],
                 "html_text": {}, "basename_map": {}}
    html_files = []
    basename_map: dict = {}   # {basename_lower: [abs_path, ...]}
    for root, _, files in os.walk(temp_dir):
        for f in files:
            lo = f.lower()
            fp = os.path.join(root, f)
            if   lo.endswith('.opf'):                      idx["opf"].append(fp)
            elif lo.endswith(('.html', '.xhtml', '.htm')): html_files.append(fp)
            elif lo.endswith('.css'):                      idx["css"].append(fp)
            elif lo.endswith('.ncx'):                      idx["ncx"].append(fp)
            # Populate basename map for all files (used by _resolve_relative_href)
            basename_map.setdefault(lo, []).append(fp)
    idx["basename_map"] = basename_map

    for fp in html_files:
        idx["html"].append(fp)
        try:
            content = read_text(fp)
            idx["html_text"][fp] = content
            if re.search(r'epub:type=["\']toc["\']', content, re.IGNORECASE):
                idx["nav"].append(fp)
        except Exception:
            idx["html_text"][fp] = ""
    _epub_index[temp_dir] = idx

def invalidate_epub_index(temp_dir: str) -> None:

    _epub_index.pop(temp_dir, None)

def _ensure_index(temp_dir: str) -> dict:

    if temp_dir not in _epub_index:
        build_epub_index(temp_dir)
    return _epub_index[temp_dir]

def _find_by_key(temp_dir: str, key: str):
    # Sorted for deterministic processing order and reproducible ZIP output.
    return sorted(p for p in _ensure_index(temp_dir)[key] if os.path.exists(p))

def find_opf(temp_dir):  return _find_by_key(temp_dir, "opf")
def find_html(temp_dir): return _find_by_key(temp_dir, "html")
def find_css(temp_dir):  return _find_by_key(temp_dir, "css")
def find_ncx(temp_dir):  return _find_by_key(temp_dir, "ncx")

def find_nav(temp_dir):
    # Simple cache lookup — NAV status resolved during build_epub_index.
    return _find_by_key(temp_dir, "nav")

def get_html_text_cache(temp_dir: str) -> dict:

    return _ensure_index(temp_dir)["html_text"]

def update_html_cache(temp_dir: str, path: str, content: str) -> None:

    idx = _ensure_index(temp_dir)
    idx["html_text"][path] = content
    # Re-check NAV status for the updated file
    nav_list = idx["nav"]
    is_nav = re.search(r'epub:type=["\']toc["\']', content, re.IGNORECASE) is not None
    if is_nav and path not in nav_list:
        nav_list.append(path)
    elif not is_nav and path in nav_list:
        nav_list.remove(path)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — METADATA CLEANING  (XML-aware, replaces trim_opf_metadata)
# ═══════════════════════════════════════════════════════════════════════════════

def _normalize_whitespace(text: str) -> str:
    if not text: return text
    return re.sub(r"\s+", " ", text).strip()

def _apply_replacements(text: str, rules: list) -> str:

    for pattern, replacement in rules:
        text = pattern.sub(replacement, text)
    return text

def _normalize_language(lang: str) -> str:
    lang = lang.strip()
    parts = re.split(r"[_\-]", lang)
    if len(parts) == 1: return parts[0].lower()
    return f"{parts[0].lower()}-{parts[1].upper()}"

def _normalize_date(date_str: str) -> str:
    date_str = date_str.strip()
    formats = [
        "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d",
        "%B %d, %Y", "%d %B %Y", "%Y",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y") if fmt == "%Y" else dt.strftime("%Y-%m-%d")
        except ValueError: continue
    return date_str

def _tag_local(element) -> str:
    tag = element.tag
    return tag.split("}", 1)[1] if "}" in tag else tag

def _ns_prefix(tag: str) -> str:
    if "}" not in tag: return ""
    uri = tag.split("}", 1)[0][1:]
    return next((p for p, u in _NS.items() if u == uri), "")


# ── Author name normalisation ──────────────────────────────────────────────────


def _title_case_name(name: str) -> str:
    words = name.split()
    result = []
    for i, word in enumerate(words):
        lower = word.lower().rstrip('.,')
        if i > 0 and lower in _NAME_PARTICLES: result.append(word.lower())
        elif lower in _NAME_SUFFIXES:
            # Preserve canonical suffix capitalisation
            result.append(word.capitalize())
        else:
            # Capitalise first letter, lowercase rest (handles McName via simple rule)
            result.append(word.capitalize())
    return ' '.join(result)

def _normalize_author_name(name: str) -> tuple[str, str]:
    # Returns (display_name, file_as). Fixes casing and inverted "Last, First" form.
    name = _normalize_whitespace(name)
    if not name: return name, name

    # ── Pre-clean: strip editor notation and trailing credentials ─────────────
    # Must happen before comma-split so credentials after a comma are not
    # mistaken for the "Last" part of an inverted name.
    name = _AUTHOR_EDITOR_RE.sub('', name).strip()
    # Strip credentials iteratively — some names have both M.D. and Ph.D.
    prev = None
    while prev != name:
        prev = name
        name = _AUTHOR_CREDENTIAL_RE.sub('', name).strip()
    name = name.rstrip(',').strip()
    if not name: return name, name

    # ── Detect "Last, First" inverted form ────────────────────────────────────
    # Guard: a trailing comma followed by a known name suffix (Jr., Sr., III)
    # is part of the name, not an inversion separator.
    # e.g. "Patterson, William H., Jr." has TWO commas:
    #   split on first comma → last="Patterson", first="William H., Jr."
    # That is the correct split — we just need to keep "Jr." with first parts.
    if ',' in name:
        parts = [p.strip() for p in name.split(',', 1)]
        last, first = parts[0], parts[1]
        # Heuristic: if "last" looks like a real surname (capitalised word, no
        # spaces, not a credential/suffix token) treat it as inverted form.
        if (last and ' ' not in last
                and last[0].isupper()
                and last.lower().rstrip('.') not in _NAME_SUFFIXES):
            display = _title_case_name(f"{first} {last}".strip())
            file_as = f"{_title_case_name(last)}, {_title_case_name(first)}".strip(', ')
            return display, file_as

    # ── Normal "First Last" (or all-caps / all-lowercase) form ───────────────
    tokens = name.split()
    if len(tokens) == 1:
        display = _title_case_name(tokens[0])
        return display, display

    # Determine whether the name is already correctly cased
    # (at least one alphabetic token has mixed case → assume intentional)
    alpha_tokens = [t for t in tokens if t.isalpha()]
    is_all_caps  = bool(alpha_tokens) and all(t.isupper() for t in alpha_tokens)
    is_all_lower = bool(alpha_tokens) and all(t.islower() for t in alpha_tokens)
    needs_casing = is_all_caps or is_all_lower

    if needs_casing: display = _title_case_name(name)
    else: display = name  # trust existing casing

    # Build file_as: last meaningful token (excluding suffixes) becomes surname.
    # Particles preceding the last name stay with it.
    # Note: compound surnames like "García Márquez" are ambiguous without a
    # language-specific database. We use last-token heuristic, which is correct
    # for the vast majority of Western names. Users with compound surnames
    # should set opf:file-as manually after this pass.
    meaningful = [t for t in tokens if t.lower().rstrip('.,') not in _NAME_SUFFIXES]
    suffixes   = [t for t in tokens if t.lower().rstrip('.,') in _NAME_SUFFIXES]

    if len(meaningful) == 1: file_as = display
    else:
        # Walk backwards until we hit a non-particle token
        surname_start = len(meaningful) - 1
        while surname_start > 0 and meaningful[surname_start - 1].lower() in _NAME_PARTICLES:
            surname_start -= 1
        first_parts   = meaningful[:surname_start]
        surname_parts = meaningful[surname_start:]
        if needs_casing:
            surname_parts = [_title_case_name(p) for p in surname_parts]
            first_parts   = [_title_case_name(p) for p in first_parts]
        surname_str = ' '.join(surname_parts)
        first_str   = ' '.join(first_parts + suffixes)
        file_as = f"{surname_str}, {first_str}".strip(', ')

    return display, file_as


# ── Publisher name normalisation ───────────────────────────────────────────────
#
# Common issues:
#   - ALL-CAPS names from older tools:  "PENGUIN BOOKS" → "Penguin Books"
#   - Trailing legal suffixes:    "HarperCollins Publishers, Inc." → "HarperCollins Publishers"
#   - Excessive whitespace (handled by _normalize_whitespace already)
#
# We are conservative: only fix all-caps. Mixed-case names like
# "HarperCollins" or "O'Reilly Media" are left alone.


def _title_case_publisher(name: str) -> str:
    # Title-case a publisher name, keeping short connector words lowercase.
    words = name.split()
    result = []
    for i, word in enumerate(words):
        if i == 0 or word.lower() not in _PUB_LOWERCASE_WORDS: result.append(word.capitalize())
        else: result.append(word.lower())
    return ' '.join(result)

def _normalize_publisher_name(name: str, strip_legal: bool = False) -> str:
    # Normalise a dc:publisher value.
    # - Fixes all-caps names to Title Case
    # - Optionally strips trailing legal entity suffixes (Inc., LLC, etc.)
    name = _normalize_whitespace(name)
    if not name: return name

    if strip_legal: name = _PUBLISHER_LEGAL_SUFFIXES.sub('', name).strip()

    # Only apply title-casing if name appears to be all-caps
    tokens = [t for t in name.split() if t.isalpha()]
    if tokens and all(t.isupper() for t in tokens): name = _title_case_publisher(name)

    return name


def _fuzzy_correct_publisher(name: str, threshold: float = 0.88) -> str:
        # Fuzzy-correct publisher typos via difflib. Threshold: ≥88% + ≤3 char diff.
    from difflib import SequenceMatcher
    name_lower = name.lower()
    best_ratio, best_canon = 0.0, None
    for _pattern, canonical in _IMPRINT_MAP:
        ratio = SequenceMatcher(None, name_lower, canonical.lower(), autojunk=False).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_canon = canonical
    if (best_ratio >= threshold and best_canon is not None
            and abs(len(name) - len(best_canon)) <= 3):
        return best_canon
    return name




# ── Publisher extended cleaning ─────────────────────────────────────────────


def _is_publisher_garbage(name: str) -> bool:
    # Return True if the publisher value should be blanked rather than normalised.
    if not name: return False
    if len(name) > 300: return True   # Goodreads JS tooltip blob or other runaway string
    if _PUBLISHER_GARBAGE_RE.search(name): return True   # JS blob, fanfic site URL, forum link
    # Bare URL (http/https/www) or fanfic site even without full URL
    if re.match(r'https?://|^www\.', name, re.I): return True
    return False


def _clean_publisher_extended(name: str,
                               consolidate: bool = False,
                               strip_extra: bool = False,
                               preserve_genre: bool = False) -> str:
    # Extended publisher name cleaning applied after basic normalisation.
    #
    # Pipeline (in order):
    # 1. Return empty string for garbage values (JS blobs, URLs, fanfic sites).
    # 2. Extract publisher from city-prefixed bibliographic strings
    # e.g. "New York : Tor, 2008." → "Tor"
    # 3. Apply typo corrections (Houghton Miffin → Houghton Mifflin, etc.)
    # 4. Normalise "and" → "&" for known publisher names.
    # 5. Strip bare URLs embedded in name.
    # 6. Strip parentheticals e.g. "(US Edition)".
    # 7. Strip "& Co." suffix.
    # 8. Strip legal entity suffixes (Inc., LLC, Ltd., Incorporated, etc.)
    # — always on; catches Pty Ltd, Pty Limited, ULC, etc.
    # 9. Optionally strip verbose business suffixes (Group, Publishing …)
    # 10. Optionally consolidate imprints to parent publisher.
    # ── 1. Garbage check ─────────────────────────────────────────────────────
    if _is_publisher_garbage(name): return ''

    # ── 2. City-prefix extraction ─────────────────────────────────────────────
    m = _CITY_PREFIX_RE.match(name.strip())
    if m: name = m.group(1).strip().rstrip('.')

    # ── 3. Typo corrections ───────────────────────────────────────────────────
    name = _apply_replacements(name, _PUBLISHER_TYPO_MAP)

    # ── 3b. Fuzzy typo correction (opt-in) ────────────────────────────────────
    # Runs after the exact typo map so known corrections don't go through the
    # slower fuzzy path.  Only triggers when the name wasn't already fixed above.
    if PUBLISHER_FUZZY_CORRECT:
        corrected = _fuzzy_correct_publisher(name)
        if corrected != name:
            name = corrected

    # ── 4. "and" → "&" normalisation ─────────────────────────────────────────
    for pattern, replacement in _PUB_AMP_NORM:
        if pattern.search(name):
            name = pattern.sub(replacement, name)
            break

    # ── 5–8. Standard stripping ───────────────────────────────────────────────
    name = _URL_RE.sub('', name).strip()
    name = _PUB_PAREN_RE.sub('', name).strip()
    name = _PUB_AND_CO_RE.sub('', name).strip()
    # Strip mid-string "Pty" before legal suffix so "Pty, Limited" is caught
    name = _PUB_PTY_RE.sub(' ', name).strip()
    name = _PUBLISHER_LEGAL_SUFFIXES.sub('', name).strip()
    name = _normalize_whitespace(name)

    # ── 9. Imprint consolidation (before verbose suffix strip to keep "Tor Books") ─
    if consolidate:
        # Short-circuit: if the current name is already a genre-defining imprint
        # and we're asked to preserve them, skip the parent-lookup entirely.
        # This handles e.g. "Tor Books" → strip suffix → "Tor" → already in
        # _GENRE_DEFINING_IMPRINTS → return "Tor" without mapping to "Macmillan".
        if preserve_genre and name in _GENRE_DEFINING_IMPRINTS:
            pass  # leave name as-is
        else:
            for pattern, replacement in _IMPRINT_MAP:
                if pattern.search(name):
                    # If the canonical replacement is itself a genre imprint and
                    # preserve_genre is on, keep the intermediate canonical name
                    # (e.g. "Tachyon" → "Tachyon Publications") not the corporate parent.
                    if preserve_genre and replacement in _GENRE_DEFINING_IMPRINTS:
                        name = replacement
                        break
                    name = replacement
                    break

    # ── 10. Verbose suffix strip ──────────────────────────────────────────────
    if strip_extra:
        name = _PUB_SUFFIX_RE.sub('', name).strip()
        name = _normalize_whitespace(name)

    return name


# ── Title normalisation ────────────────────────────────────────────────────────
#
# What we fix:
#   1. ALL-CAPS titles:    "THE GREAT GATSBY"       → "The Great Gatsby"
#   2. all-lowercase:      "the great gatsby"        → "The Great Gatsby"
#   3. Blank opf:file-as:  "The Hobbit"              → file-as "Hobbit, The"
#      Leading articles (The, A, An — in 30+ languages) are moved to the end.
#
# What we deliberately leave alone:
#   - Already mixed-case titles ("Gone with the Wind", "To Kill a Mockingbird")
#     — mixed case is assumed intentional; we can't know the author's style intent.
#   - Subtitle capitalisation after ":" — style-guide dependent.
#   - Non-Latin scripts — we skip normalisation if no ASCII-alpha tokens are found.
#
# The opf:file-as sort key IS always regenerated when normalize_titles=True,
# because a missing or stale sort key is unambiguously wrong regardless of casing.


def _title_case_title(title: str) -> str:
    # Apply title-case to a book title string.
    # First word and words after : or — are always capitalised.
    # Short function words are kept lowercase elsewhere.
    # Mixed tokens (e.g. 'iPhone', 'McBain') are left as-is.
    words  = title.split()
    result = []
    force_cap = True   # first word always capitalised

    for word in words:
        # Strip leading/trailing punctuation for analysis but preserve it
        core = word.strip('.,;:!?()\'"')
        lower_core = core.lower()

        if force_cap or lower_core not in _TITLE_LOWERCASE_WORDS:
            # Only apply capitalise() if the token is all-one-case
            # (preserves intentional mixed-case like "iPhone" or "McBain")
            if core.isupper() or core.islower():
                result.append(word.replace(core, core.capitalize(), 1))
            else: result.append(word)  # leave mixed-case token alone
        else: result.append(word.lower())

        # Force capitalisation after sentence-ending punctuation in the title
        force_cap = word.endswith((':', '—', '–'))

    return ' '.join(result)


def _title_sort_key(title: str) -> str:
    # Derive an opf:file-as sort key by moving a leading article to the end.
    # "The Hobbit"          → "Hobbit, The"
    # "A Tale of Two Cities"→ "Tale of Two Cities, A"
    # "Gone with the Wind"  → "Gone with the Wind"   (no leading article)
    # "1984"                → "1984"                  (no article)
    title = _normalize_whitespace(title)
    if not title: return title

    words = title.split()
    if len(words) < 2: return title

    first = words[0].rstrip('.,')
    if first.lower() in _TITLE_ARTICLES:
        rest = ' '.join(words[1:])
        return f"{rest}, {first}"

    return title


def _normalize_title(title: str) -> tuple[str, str]:
    # Normalise a dc:title display value and derive its opf:file-as sort key.
    #
    # Returns (display_title, file_as).
    #
    # Only alters casing when the entire title is detectably wrong (all-caps
    # or all-lowercase). Mixed-case titles are returned unchanged.
    title = _normalize_whitespace(title)
    if not title: return title, title

    # Check whether the title needs casing correction.
    # We only look at alphabetic tokens and skip if there aren't any
    # (e.g. "1984" or "\u5c0f\u8aac\u306e\u30bf\u30a4\u30c8\u30eb" — numbers-only or non-Latin).
    alpha_tokens = [t for t in title.split() if any(c.isalpha() for c in t)]
    if not alpha_tokens: return title, _title_sort_key(title)

    is_all_caps  = all(t.upper() == t for t in alpha_tokens)
    is_all_lower = all(t.lower() == t for t in alpha_tokens) and len(alpha_tokens) > 1

    if is_all_caps or is_all_lower: display = _title_case_title(title)
    else: display = title   # trust existing mixed casing

    return display, _title_sort_key(display)


def _clean_title_tags(title: str) -> str:
    # 1. Strip trailing parentheticals and brackets.
    title = _TITLE_TRAILING_PAREN_RE.sub('', title).strip()
    title = _TITLE_TRAILING_BRACKET_RE.sub('', title).strip()

    # 2. Strip generic genre suffixes (dash and colon forms) before the
    #    inversion check so "Painted Queen, The: A Novel" becomes
    #    "Painted Queen, The" and the inversion regex can then match cleanly.
    title = _TITLE_GENRE_SUFFIX_RE.sub('', title).strip()
    title = _TITLE_COLON_GENRE_RE.sub('', title).strip()

    # 3. Fix "X, The" sort-form inversion → "The X".
    m = _TITLE_THE_INVERSION_RE.match(title)
    if m:
        article = m.group(2).capitalize()   # 'the' → 'The'
        title = f"{article} {m.group(1).strip()}"

    return _normalize_whitespace(title)


# ── Subject tag normalisation ─────────────────────────────────────────────────────────────────────


def _normalize_subjects(subjects: list, max_count: int = 5) -> list:
    # Clean and normalise a list of dc:subject tags.
    #
    # Pipeline per subject (in order):
    # 1.  Strip HTML tags and decode entities.
    # 2.  Drop BISAC machine codes (BIO000000, sci_biology, sf_fantasy …).
    # 3.  Strip library "—Fiction" suffix: "Amish—Fiction" → "Amish".
    # Whole-string exceptions: Non-Fiction, Science-Fiction, Fan-Fiction.
    # 4.  Drop personal shelving / management noise and blocklisted values.
    # 5.  Drop multi-word subjects that are NOT hyphenated.
    # "Science Fiction" → dropped.  "Science-Fiction" → kept.
    # 6.  Drop subjects shorter than 3 characters (after markup stripping).
    # Catches single-letter tags (r, s, t …), two-char abbreviations
    # (SF, YA, UK …) and other micro-tokens with no subject meaning.
    # 7.  Drop subjects that contain ANY non-alphabetic character other than
    # a hyphen between letters.  This blocks: slashes ("Fiction/Adventure"),
    # ampersands ("Horror & Thriller"), digits ("9781234567890", "2024"),
    # hashtags ("#genre"), underscores ("sci_fi"), BISAC dots, parens, etc.
    # Apostrophes in possessives ("Children's", "Women's") are still allowed
    # because the apostrophe is surrounded by letters on both sides.
    # 8.  Apply typo corrections (looked up by lowercase key).
    # 9.  Normalise casing: first letter uppercase, rest lowercase.
    # 10.  Deduplicate case-insensitively; also deduplicate singular/plural pairs
    # ("Alien" and "Aliens" → keep whichever arrived first).
    # 11.  Cap at max_count.
    seen_lower: set = set()
    result: list   = []

    for subj in subjects:
        # ── 1. Strip markup ──────────────────────────────────────────────────
        s = re.sub(r'<[^>]+>', '', subj)
        s = re.sub(r'&[a-zA-Z]+;|&#\d+;|&#x[0-9a-fA-F]+;', ' ', s)
        s = _normalize_whitespace(s)
        if not s: continue

        # ── 2. BISAC machine codes ───────────────────────────────────────────
        if _SUBJECT_BISAC_RE.match(s): continue

        # ── 3. Strip "—Fiction" suffix ───────────────────────────────────────
        if not _SUBJECT_FICTION_WHOLE.match(s): s = _SUBJECT_FICTION_SUFFIX_RE.sub('', s).strip()
        if not s: continue

        # ── 4. Blocklist (case-insensitive) ──────────────────────────────────
        if s.lower() in _SUBJECT_BLOCKLIST: continue

        # ── 5. Drop unhyphenated multi-word subjects ─────────────────────────
        if ' ' in s: continue

        # ── 6. Minimum length: 3 characters ──────────────────────────────────
        if len(s) < 3: continue

        # ── 7. Drop subjects with any non-alphabetic character ───────────────
        #    Allowed interior chars: hyphen between letters, apostrophe between
        #    letters (e.g. "Children's").  Everything else is rejected.
        #
        #    We first check for the fast-path: purely alphabetic (most common).
        #    If not, we allow only hyphens and apostrophes that are *surrounded*
        #    by letters on both sides.
        if not s.isalpha():
            # Remove allowed interior connectors, then check what remains
            stripped = re.sub(r"(?<=[a-zA-Z])[-'](?=[a-zA-Z])", '', s)
            if not stripped.isalpha(): continue  # contains digits, slashes, &, parens, _, #, etc.

        # ── 8. Typo corrections ───────────────────────────────────────────────
        corrected = _SUBJECT_TYPO_MAP.get(s.lower())
        if corrected: s = corrected

        # ── 9. Normalise casing ──────────────────────────────────────────────
        #    First char uppercase, rest lowercase.
        #    "THRILLER"        → "Thriller"
        #    "Post-Apocalyptic"→ "Post-apocalyptic"
        #    "Children's"      → "Children's"
        #    Typo corrections already set the canonical form, so we lowercase
        #    after substitution which is safe (Apocalypse→Apocalypse, etc.).
        s = s[0].upper() + s[1:].lower()

        # ── 10. Deduplicate ───────────────────────────────────────────────────
        key = s.lower()
        # Also treat singular/plural as duplicates: if "Alien" is already in
        # and we see "Aliens", skip it (and vice-versa).
        if key in seen_lower: continue
        # Treat singular/plural as duplicates.
        # Strip common plural endings to get a stem, then check both ways.
        # "Witches" → stem "witch"; "Aliens" → stem "alien".
        stem = key
        if stem.endswith('ches'): stem = stem[:-2]   # witches → witch
        elif stem.endswith('es') and len(stem) > 4: stem = stem[:-2]   # aliases → alias
        elif stem.endswith('s') and len(stem) > 3: stem = stem[:-1]   # aliens → alien
        if stem != key and stem in seen_lower: continue
        if (key + 's') in seen_lower: continue
        if (key + 'es') in seen_lower: continue
        seen_lower.add(key)
        result.append(s)

        # ── 11. Cap ──────────────────────────────────────────────────────────
        if len(result) >= max_count: break

    return result


def _normalize_marc_role(role: str) -> str: return _MARC_ROLE_MAP.get(role.strip().lower(), role)


def _clean_description(text: str) -> str:
    # Single-pass entity replacement using pre-compiled regex — O(n) not O(n × m)
    text = _HTML_ENTITIES_RE.sub(lambda m: _HTML_ENTITIES[m.group(0)], text)
    text = _DESC_ENTITY_RE.sub(' ', text)
    text = _DESC_HTML_TAG_RE.sub(' ', text)
    return _normalize_whitespace(text)


# ── ISBN validation and normalization  (Feature H) ────────────────────────────

def _isbn13_checksum_ok(digits: str) -> bool:
    # Return True if the 13-digit string passes the ISBN-13 check digit.
    if len(digits) != 13 or not digits.isdigit(): return False
    total = sum(int(d) * (1 if i % 2 == 0 else 3) for i, d in enumerate(digits[:12]))
    check = (10 - (total % 10)) % 10
    return check == int(digits[12])


def _isbn10_checksum_ok(digits: str) -> bool:
    # Return True if the 10-character string passes the ISBN-10 check digit.
    if len(digits) != 10: return False
    if not digits[:9].isdigit(): return False
    total = sum(int(digits[i]) * (10 - i) for i in range(9))
    check_char = digits[9].upper()
    check_val  = 11 - (total % 11)
    if check_val == 10: return check_char == 'X'
    if check_val == 11: return check_char == '0'
    return check_char == str(check_val)


def _normalize_isbn(raw: str) -> tuple[str | None, bool]:
    # Given a raw identifier string, attempt to detect and normalize an ISBN.
    #
    # Returns (normalized_isbn_string, checksum_valid) or (None, False) if the
    # string does not look like an ISBN.
    #
    # Normalization:
    # • Strip 'urn:isbn:' / 'isbn:' prefixes
    # • Remove hyphens and spaces
    # • Reformat as: ISBN 978-X-XXXX-XXXX-X  (hyphenated groups not added —
    # group rules are complex; we just return the bare 13-digit number)
    s = raw.strip().lower()
    for prefix in ('urn:isbn:', 'isbn:', 'isbn'):
        if s.startswith(prefix):
            s = s[len(prefix):].strip()
            break
    # Remove hyphens and spaces that might be part of formatting
    digits = re.sub(r'[-\s]', '', s).upper()
    if len(digits) == 13 and digits.isdigit():
        ok = _isbn13_checksum_ok(digits)
        return (f'urn:isbn:{digits}', ok)
    if len(digits) == 10 and (digits[:9].isdigit() and digits[9] in '0123456789X'):
        ok = _isbn10_checksum_ok(digits)
        return (f'urn:isbn:{digits}', ok)
    return (None, False)


def _scrub_print_artifacts(html: str) -> str:
    for pattern in _PRINT_ARTIFACT_RES: html = pattern.sub('', html)
    return html


def _strip_deprecated_html_attrs(html: str) -> str: return _DEPRECATED_ATTRS_RE.sub('', html)


def _strip_body_background(css: str) -> str: return _BODY_BG_COLOR_RE.sub(r'\1', css)


def clean_opf_metadata_xml(opf_path: str,
                            remove_fields: list = None,
                            normalize_authors: bool = False,
                            demote_secondary_authors: bool = False,
                            normalize_publisher: bool = False,
                            strip_publisher_legal: bool = False,
                            consolidate_publisher: bool = False,
                            clean_publisher_extra: bool = False,
                            normalize_titles: bool = False,
                            clean_title_tags: bool = False,
                            normalize_subjects: bool = False,
                            max_subjects: int = 5,
                            enforce_modified_date: bool = False,
                            enforce_single_identifier: bool = False,
                            clean_description_html: bool = False,
                            normalize_marc_roles: bool = False,
                            verbose: bool = False) -> list:
    # XML-aware metadata cleaning for a single OPF file on disk.
    #
    # Returns a list of (field_label, before, after) change tuples.
    # Writes the file in-place only if changes were made.
    remove_fields = [f.lower() for f in (remove_fields or [])]
    changes = []

    try:
        raw  = read_raw(opf_path)
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        print(f"  Warning: could not parse OPF {opf_path}: {e}", file=sys.stderr)
        return changes

    # Find <metadata> element (with or without namespace)
    metadata_el = (root.find(f"{{{_NS['opf']}}}metadata")
                   or root.find("metadata"))
    if metadata_el is None: return changes

    # Detect EPUB version so we can gate EPUB3-only metadata elements
    _epub_version = (root.get("version") or "2.0").strip()
    _is_epub3     = _epub_version.startswith("3")

    opf_ns     = _NS["opf"]
    dc_ns      = _NS["dc"]
    to_remove  = []
    seen_dc    = {}

    # ── Pre-pass: collect all dc:creator elements for demotion logic ───────────
    # We need the full list before we start iterating so we can pick the primary.
    all_creators = [
        c for c in metadata_el
        if _tag_local(c) == "creator" and _ns_prefix(c.tag) == "dc"
    ]

    def _is_primary_author(el):
        # True if element has an explicit aut role in EPUB2 or EPUB3 style.
        # EPUB2: opf:role="aut"
        role_attr = el.get(f"{{{opf_ns}}}role") or el.get("opf:role") or ""
        if role_attr.lower() == "aut": return True
        # EPUB3: <meta refines="#id" property="role" scheme="marc:relators">aut</meta>
        el_id = el.get("id")
        if el_id:
            for meta in metadata_el:
                if (_tag_local(meta) == "meta"
                        and meta.get("refines") == f"#{el_id}"
                        and meta.get("property") == "role"
                        and (meta.text or "").strip().lower() == "aut"):
                    return True
        return False

    # Determine which creator is the primary (keep as dc:creator)
    primary_creator = None
    if demote_secondary_authors and len(all_creators) > 1:
        # Prefer an explicit aut role; fall back to first in document order
        for c in all_creators:
            if _is_primary_author(c):
                primary_creator = c
                break
        if primary_creator is None: primary_creator = all_creators[0]

    # ── Main pass ──────────────────────────────────────────────────────────────
    for child in list(metadata_el):
        local       = _tag_local(child)
        prefix      = _ns_prefix(child.tag)
        field_label = f"{prefix}:{local}" if prefix else local

        # ── 1. Strip vendor meta elements ──────────────────────────────────
        if prefix == "" and local == "meta":
            name_attr = (child.get("name") or "").lower()
            # Catch calibre:*, adobe:*, ibooks:*, kobo:*, sigil:* vendor tags
            if any(name_attr.startswith(vp) for vp in VENDOR_NS_PREFIXES):
                if name_attr == 'calibre:rating':
                    # Log rating removal at NOTE level (always visible at default)
                    changes.append(('calibre:rating', child.get('content', ''), '(removed)'))
                elif verbose: print(f"    Remove vendor meta: name='{name_attr}'")
                to_remove.append(child)
                continue
            # Also catch bare "rating" or "star-rating" meta tags
            if name_attr in ('rating', 'star-rating'):
                if verbose: print(f"    Remove rating meta: name='{name_attr}'")
                to_remove.append(child)
                continue

        # ── 2. Remove explicitly requested fields ───────────────────────────
        if field_label.lower() in remove_fields:
            if verbose: print(f"    Remove field: {field_label}")
            to_remove.append(child)
            continue

        # ── 3. Clean text content ───────────────────────────────────────────
        if child.text:
            orig = child.text

            if local == "language": child.text = _normalize_language(child.text)

            elif local in ("date", "modified") or field_label in ("dc:date", "dcterms:modified"):
                child.text = _normalize_date(child.text)

            elif local == "creator" and prefix == "dc" and normalize_authors:
                display, file_as = _normalize_author_name(child.text)
                child.text = display
                # Update or create opf:file-as attribute
                fa_key = f"{{{opf_ns}}}file-as"
                old_fa = child.get(fa_key) or child.get("opf:file-as") or ""
                if old_fa != file_as:
                    child.set(fa_key, file_as)
                    if old_fa: changes.append((f"{field_label}[@opf:file-as]", old_fa, file_as))
                    else: changes.append((f"{field_label}[@opf:file-as]", "(none)", file_as))

            elif local == "publisher" and prefix == "dc" and normalize_publisher:
                # Garbage check runs unconditionally — JS blobs, URLs, fanfic
                # sites should be blanked regardless of other flags.
                if _is_publisher_garbage(child.text): child.text = ''
                else:
                    child.text = _normalize_publisher_name(
                        child.text, strip_legal=strip_publisher_legal)
                    # Extended cleaning: city-prefix, typos, amp-norm,
                    # parentheticals, imprint consolidation.
                    # Always runs when normalize_publisher is on; the
                    # consolidate/strip_extra flags gate the optional steps.
                    child.text = _clean_publisher_extended(
                        child.text,
                        consolidate=consolidate_publisher,
                        strip_extra=clean_publisher_extra,
                        preserve_genre=PRESERVE_GENRE_IMPRINTS,
                    )

            elif local == "title" and prefix == "dc" and normalize_titles:
                if clean_title_tags: child.text = _clean_title_tags(child.text)
                display, file_as = _normalize_title(child.text)
                child.text = display
                # Update or create opf:file-as sort key
                fa_key = f"{{{opf_ns}}}file-as"
                old_fa = child.get(fa_key) or child.get("opf:file-as") or ""
                if old_fa != file_as:
                    child.set(fa_key, file_as)
                    if old_fa: changes.append((f"{field_label}[@opf:file-as]", old_fa, file_as))
                    else: changes.append((f"{field_label}[@opf:file-as]", "(none)", file_as))

            else:
                if local == "description" and prefix == "dc":
                    # Always strip HTML tags — they are invalid in OPF and come
                    # from retail export tools.  The full entity/tag clean runs
                    # when clean_description_html=True; a lighter strip always runs.
                    if clean_description_html: child.text = _clean_description(child.text)
                    else:
                        # Minimal always-on: strip obvious HTML tags + truncate
                        text = re.sub(r'<[^>]+>', ' ', child.text)
                        text = _normalize_whitespace(text)
                        if len(text) > 2000:
                            cut = text.rfind('.', 0, 2000)
                            text = (text[:cut + 1] if cut > 1500 else text[:2000]).strip()
                        child.text = text
                elif local == "rights" and prefix == "dc":
                    # Truncate retail rights boilerplate (often 300–500 chars).
                    # Keep first sentence or first 150 chars, whichever is shorter.
                    text = _normalize_whitespace(child.text)
                    if len(text) > 150:
                        cut = text.find('.', 0, 150)
                        text = (text[:cut + 1] if 20 < cut <= 150 else text[:150]).strip()
                    child.text = text
                else: child.text = _normalize_whitespace(child.text)

            if orig != child.text:
                changes.append((field_label, orig, child.text))
                if verbose: print(f"    [{field_label}] '{orig}' → '{child.text}'")

        # ── 4. Remove element if now empty ──────────────────────────────────
        # Special case: OPF <meta> elements carry their value in attributes
        # (name=, content=, property=) rather than text content, so we cannot
        # use empty-text as the deletion signal for all of them.
        # We KEEP metas that carry known-functional EPUB signals:
        #   • name="cover"    — EPUB2 cover image hint (critical for macOS Preview)
        #   • name="viewport" — layout hint used by some reading systems
        #   • property=*      — EPUB3 structured metadata
        # Everything else with no text is vendor noise (FB2, FB2EPUB, etc.)
        # and is intentionally removed.
        if not (child.text or "").strip():
            if local == "meta":
                meta_name = (child.get("name") or child.get(f"{{{opf_ns}}}name") or "").lower()
                meta_prop = child.get("property") or ""
                # Preserve if it carries a known-functional EPUB signal
                if meta_name in ("cover", "viewport") or meta_prop: pass  # keep this meta
                else:
                    if verbose: print(f"    Remove empty meta: {field_label}[@name='{meta_name}']")
                    to_remove.append(child)
                    continue
            else:
                if verbose: print(f"    Remove empty: {field_label}")
                to_remove.append(child)
                continue

        # ── 5. Remove exact DC duplicates (and language case-variants) ────────
        if prefix == "dc":
            # For dc:language, normalise case before dedup so "en", "EN", "En"
            # are treated as identical (item 14 — language case-variant dedup).
            dedup_text = child.text.lower() if local == "language" else child.text
            key = (local, dedup_text)
            if key in seen_dc:
                if verbose: print(f"    Remove duplicate: {field_label} = '{child.text}'")
                to_remove.append(child)
                continue
            seen_dc[key] = True

        # ── 6. Demote secondary dc:creator → dc:contributor ─────────────────
        if (demote_secondary_authors
                and local == "creator"
                and prefix == "dc"
                and len(all_creators) > 1
                and child is not primary_creator):
            old_tag = child.tag
            child.tag = f"{{{dc_ns}}}contributor"
            # Ensure role attribute is set to something meaningful if missing
            role_key = f"{{{opf_ns}}}role"
            if not child.get(role_key) and not child.get("opf:role"):
                child.set(role_key, "ctb")  # "ctb" = contributor in MARC relators
            changes.append(("dc:creator→dc:contributor", child.text or "", child.text or ""))
            if verbose: print(f"    Demote secondary author to contributor: '{child.text}'")
            continue  # skip further processing for this element this pass

        # ── 7. Clean attribute values + MARC role normalisation ─────────────
        for attr_name, attr_val in list(child.attrib.items()):
            cleaned = _normalize_whitespace(attr_val)
            # Normalise opf:role to 3-letter MARC code
            if normalize_marc_roles and attr_name in (
                    f"{{{opf_ns}}}role", "opf:role", "role"):
                marc = _normalize_marc_role(cleaned)
                if marc != cleaned:
                    changes.append((f"{field_label}[@{attr_name}]", cleaned, marc))
                    child.attrib[attr_name] = marc
                    cleaned = marc
            if cleaned != attr_val and child.attrib.get(attr_name) == attr_val:
                changes.append((f"{field_label}[@{attr_name}]", attr_val, cleaned))
                child.attrib[attr_name] = cleaned

    # ── Default role on dc:creator with missing role ────────────────────────
    if normalize_marc_roles:
        for c in all_creators:
            role_key  = f"{{{opf_ns}}}role"
            role_attr = c.get(role_key) or c.get("opf:role") or ""
            if not role_attr.strip():
                c.set(role_key, "aut")
                changes.append(("dc:creator[@opf:role]", "(none)", "aut"))
                if verbose: print(f"    Set default role aut on: '{c.text}'")

    for el in to_remove: metadata_el.remove(el)

    # ── Sweep: remove <meta refines="#id"> orphans ───────────────────────────
    # When dc:creator / dc:contributor / dc:publisher elements are removed
    # (by remove_fields, dedup, or demotion), any <meta refines="#their-id">
    # children become broken references in the EPUB3 metadata graph.
    # Collect the ids of all still-present metadata elements and remove any
    # <meta refines="…"> that points to a now-absent id.
    surviving_ids = {
        el.get("id") for el in list(metadata_el)
        if el.get("id") is not None
    }
    orphan_refines = [
        el for el in list(metadata_el)
        if (_tag_local(el) == "meta"
            and el.get("refines")
            and el.get("refines").lstrip("#") not in surviving_ids)
    ]
    for el in orphan_refines:
        metadata_el.remove(el)
        if verbose:
            print(f"    Remove dangling refines: "
                  f"property='{el.get('property')}' refines='{el.get('refines')}'")

    if normalize_subjects:
        all_subjects = [
            c for c in list(metadata_el)
            if _tag_local(c) == "subject" and _ns_prefix(c.tag) == "dc"
        ]
        if all_subjects:
            orig_texts  = [c.text or "" for c in all_subjects]
            kept_texts  = _normalize_subjects(orig_texts, max_count=max_subjects)
            # Remove all subject elements
            for c in all_subjects: metadata_el.remove(c)
            # Re-insert only the kept ones in order, after the last metadata child
            ref_el = list(metadata_el)[-1] if list(metadata_el) else None
            for text in kept_texts:
                new_el = ET.SubElement(metadata_el, f"{{{_NS['dc']}}}subject")
                new_el.text = text
            removed_count = len(orig_texts) - len(kept_texts)
            if removed_count > 0 or orig_texts != kept_texts:
                changes.append(("dc:subject[normalised]",
                                 f"{len(orig_texts)} tags",
                                 f"{len(kept_texts)} tags"))
                if verbose:
                    print(f"    Subjects: {len(orig_texts)} → {len(kept_texts)}: {kept_texts}")

    # ── Post-pass: dcterms:modified enforcement ──────────────────────────────
    # dcterms:modified is an EPUB3-only element (OPF 3.0 §3.4.1).
    # We skip injection for EPUB2 books to keep the OPF spec-conformant.
    # Since generate_epub3_nav() always creates a NAV and repair_opf_metadata()
    # then upgrades version to 3.0, most books will be EPUB3 by the time
    # clean_opf_metadata_xml() is called — but metadata cleaning runs first in
    # the pipeline, so we must check the version as it exists in the file, not
    # as it will be after the structural repair pass.
    if enforce_modified_date and _is_epub3:
        modified_els = [
            c for c in list(metadata_el)
            if ((_tag_local(c) == "modified" and _ns_prefix(c.tag) == "dcterms")
                or (c.get("property", "") == "dcterms:modified"))
        ]
        now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        # Remove all existing modified elements
        for c in modified_els: metadata_el.remove(c)
        # Insert exactly one
        new_mod = ET.SubElement(metadata_el, f"{{{_NS['dcterms']}}}modified")
        new_mod.text = now_iso
        if len(modified_els) != 1 or (modified_els and modified_els[0].text != now_iso):
            changes.append(("dcterms:modified",
                             modified_els[0].text if modified_els else "(none)",
                             now_iso))
            if verbose: print(f"    dcterms:modified enforced: {now_iso}")
    elif enforce_modified_date and not _is_epub3:
        # For EPUB2: preserve any existing dc:date but don't inject dcterms:modified
        if verbose:
            print(f"    dcterms:modified skipped (EPUB2 — will be enforced after upgrade to 3.0)")

    # ── Post-pass: single primary identifier ────────────────────────────────
    if enforce_single_identifier:
        all_ids = [
            c for c in list(metadata_el)
            if _tag_local(c) == "identifier" and _ns_prefix(c.tag) == "dc"
        ]
        if len(all_ids) > 1:
            # Prefer ISBN (scheme attribute contains "isbn" or text starts with urn:isbn:)
            def _is_isbn(el):
                scheme = (el.get(f"{{{opf_ns}}}scheme") or
                          el.get("opf:scheme") or
                          el.get("scheme") or "").lower()
                text   = (el.text or "").lower()
                return "isbn" in scheme or text.startswith("urn:isbn:") or text.startswith("isbn")
            isbn_ids = [c for c in all_ids if _is_isbn(c)]
            keeper   = isbn_ids[0] if isbn_ids else all_ids[0]
            # Ensure keeper has id="BookId" and package unique-identifier points to it
            old_keeper_id = keeper.get("id")  # remember old id before renaming
            keeper.set("id", "BookId")
            # Re-point any <meta refines="#old-id"> to #BookId so they survive
            # the orphan-refines sweep (e.g. identifier-type, which macOS Books
            # uses for cover thumbnail lookup).
            if old_keeper_id and old_keeper_id != "BookId":
                old_ref = f"#{old_keeper_id}"
                for meta_el in list(metadata_el):
                    if (_tag_local(meta_el) == "meta"
                            and meta_el.get("refines") == old_ref):
                        meta_el.set("refines", "#BookId")
                        if verbose:
                            print(f"    Updated refines: '{old_ref}' → '#BookId' "
                                  f"(property='{meta_el.get('property')}')")
            for c in all_ids:
                if c is not keeper: metadata_el.remove(c)
            # Patch package unique-identifier attribute
            pkg_el = root.find(f"{{{opf_ns}}}package") or root
            if pkg_el.get("unique-identifier") != "BookId":
                pkg_el.set("unique-identifier", "BookId")
            count_removed = len(all_ids) - 1
            if count_removed:
                changes.append(("dc:identifier[enforced]",
                                 f"{len(all_ids)} identifiers",
                                 f"1 kept (BookId)"))
                if verbose: print(f"    Identifiers: kept '{keeper.text}', removed {count_removed}")

        # ISBN checksum validation on the surviving identifier
        surviving = [
            c for c in list(metadata_el)
            if _tag_local(c) == "identifier" and _ns_prefix(c.tag) == "dc"
        ]
        for id_el in surviving:
            raw_val = (id_el.text or "").strip()
            if not raw_val: continue
            normalized, checksum_ok = _normalize_isbn(raw_val)
            if normalized is None: continue  # not an ISBN — leave alone
            if normalized != raw_val:
                changes.append(("dc:identifier[isbn-normalized]", raw_val, normalized))
                id_el.text = normalized
                if verbose: print(f"    ISBN normalized: '{raw_val}' → '{normalized}'")
            if not checksum_ok:
                print(f"  Warning: ISBN failed checksum validation: '{raw_val}' "
                      f"(value retained — may be a pre-ISBN-13 legacy identifier)",
                      file=sys.stderr)

    # ── Post-pass: ensure cover-image manifest property ─────────────────────
    # macOS Preview / Apple Books require properties="cover-image" on the
    # actual image/* manifest item (not on an xhtml cover page).
    #
    # Resolution order for finding the cover JPEG:
    #   1. <meta name="cover" content="id"> where the item is image/*
    #   2. <meta name="cover" content="id"> where the item is xhtml — chase
    #      into that HTML file and find the first <img src="..."> to get the JPEG
    #   3. No meta name=cover — look for a manifest item whose id or href
    #      contains "cover" and whose media-type is image/*
    #
    # We also ensure <meta name="cover"> exists pointing to the JPEG item id,
    # adding one if absent (important for EPUB2 readers and macOS Preview).
    try:
        manifest_el = (root.find(f"{{{opf_ns}}}manifest") or root.find("manifest"))
        if manifest_el is not None:
            # Build id→item and href→item lookup tables
            id_to_item  = {it.get("id"): it for it in manifest_el if it.get("id")}
            href_to_item = {it.get("href"): it for it in manifest_el if it.get("href")}

            def _is_image_item(item_el):
                mt = (item_el.get("media-type") or "").lower()
                return mt.startswith("image/")

            def _find_img_href_in_html(html_arc_href):
                # Open an HTML file (relative to OPF dir) and return the first img src.
                try:
                    html_abs = os.path.join(os.path.dirname(opf_path), html_arc_href)
                    if not os.path.exists(html_abs): return None
                    html_text = read_text(html_abs)
                    m = re.search(r'<img\b[^>]+\bsrc="([^"]+)"', html_text, re.IGNORECASE)
                    if not m:
                        m = re.search(
                            r'<image\b[^>]+\bxlink:href="([^"]+)"',
                            html_text, re.IGNORECASE)
                    if not m: return None
                    # Resolve relative to HTML file's directory
                    html_dir = os.path.dirname(html_arc_href)
                    raw = m.group(1).split("?")[0].split("#")[0]
                    parts = []
                    for seg in ((html_dir + "/" + raw) if html_dir else raw).split("/"):
                        if seg == "..":
                            if parts: parts.pop()
                        elif seg != ".": parts.append(seg)
                    return "/".join(parts)
                except Exception: return None

            # Step 1 & 2: follow <meta name="cover">
            cover_jpeg_item = None
            cover_meta_el   = None
            for c in metadata_el:
                if _tag_local(c) == "meta" and (c.get("name") or "").lower() == "cover":
                    cover_meta_el = c
                    break

            if cover_meta_el is not None:
                ref_id   = cover_meta_el.get("content", "")
                ref_item = id_to_item.get(ref_id)
                if ref_item is not None:
                    if _is_image_item(ref_item):
                        # Directly points to image — ideal
                        cover_jpeg_item = ref_item
                    else:
                        # Points to an HTML page — chase into it
                        img_href = _find_img_href_in_html(ref_item.get("href", ""))
                        if img_href:
                            img_item = href_to_item.get(img_href)
                            if img_item is not None and _is_image_item(img_item):
                                cover_jpeg_item = img_item
                                # Re-point meta name=cover to the actual JPEG id
                                cover_meta_el.set("content", img_item.get("id", ref_id))
                                changes.append(("metadata:cover-meta-retarget",
                                                ref_id, img_item.get("id", ref_id)))
                                if verbose:
                                    print(f"    Re-targeted <meta name=cover> from HTML "
                                          f"id='{ref_id}' → JPEG id='{img_item.get('id')}'")

            # Step 3: no meta, or chase failed — heuristic JPEG search
            if cover_jpeg_item is None:
                for item in manifest_el:
                    if not _is_image_item(item): continue
                    item_id   = (item.get("id")   or "").lower()
                    item_href = (item.get("href")  or "").lower()
                    if "cover" in item_id or "cover" in item_href:
                        cover_jpeg_item = item
                        break

            # Now apply properties="cover-image" to the resolved JPEG item
            if cover_jpeg_item is not None:
                # Remove cover-image from any non-image items (cleanup bad prior state)
                for item in manifest_el:
                    if not _is_image_item(item):
                        props = item.get("properties", "")
                        if "cover-image" in props:
                            cleaned = re.sub(r'\bcover-image\b', "", props).strip()
                            if cleaned: item.set("properties", cleaned)
                            else: del item.attrib["properties"]
                            changes.append(("manifest:cover-image-removed-from-html",
                                            props, cleaned or "(none)"))
                            if verbose:
                                print(f"    Removed cover-image property from non-image "
                                      f"item id='{item.get('id')}'")

                # Set cover-image on the JPEG
                existing_props = cover_jpeg_item.get("properties", "")
                if "cover-image" not in existing_props:
                    new_props = (existing_props + " cover-image").strip()
                    cover_jpeg_item.set("properties", new_props)
                    jpeg_id = cover_jpeg_item.get("id", "?")
                    changes.append(("manifest:cover-image",
                                    "(none)", f"properties=cover-image on {jpeg_id}"))
                    if verbose: print(f"    Set properties=cover-image on JPEG item id='{jpeg_id}'")

                # Ensure <meta name="cover"> exists pointing to the JPEG item
                jpeg_id = cover_jpeg_item.get("id", "")
                if jpeg_id:
                    if cover_meta_el is None:
                        # No meta at all — inject one
                        new_meta = ET.SubElement(metadata_el, f"{{{opf_ns}}}meta"
                                                 if opf_ns else "meta")
                        new_meta.set("name", "cover")
                        new_meta.set("content", jpeg_id)
                        changes.append(("metadata:cover-meta-added",
                                        "(none)", f"<meta name=cover content={jpeg_id!r}>"))
                        if verbose: print(f"    Added <meta name=cover content='{jpeg_id}'>")
                    elif cover_meta_el.get("content") != jpeg_id:
                        cover_meta_el.set("content", jpeg_id)

    except Exception: pass  # never break the pipeline for a cosmetic enhancement

    # Write back only if something changed or elements were removed
    if changes or to_remove:
        declaration = '<?xml version="1.0" encoding="UTF-8"?>\n'
        new_xml = declaration + ET.tostring(root, encoding="unicode", xml_declaration=False)
        write_text(opf_path, new_xml)

    return changes


def clean_metadata(temp_dir: str,
                   remove_fields: list = None,
                   normalize_authors: bool = False,
                   demote_secondary_authors: bool = False,
                   normalize_publisher: bool = False,
                   strip_publisher_legal: bool = False,
                   consolidate_publisher: bool = False,
                   clean_publisher_extra: bool = False,
                   normalize_titles: bool = False,
                   clean_title_tags: bool = False,
                   normalize_subjects: bool = False,
                   max_subjects: int = 5,
                   enforce_modified_date: bool = False,
                   enforce_single_identifier: bool = False,
                   clean_description_html: bool = False,
                   normalize_marc_roles: bool = False,
                   verbose: bool = False,
                   epub_name: str = '') -> int:
    # Run XML-aware metadata cleaning on all OPF files. Returns total changes.
    total = 0
    for opf_path in find_opf(temp_dir):
        changes = clean_opf_metadata_xml(
            opf_path,
            remove_fields=remove_fields,
            normalize_authors=normalize_authors,
            demote_secondary_authors=demote_secondary_authors,
            normalize_publisher=normalize_publisher,
            strip_publisher_legal=strip_publisher_legal,
            consolidate_publisher=consolidate_publisher,
            clean_publisher_extra=clean_publisher_extra,
            normalize_titles=normalize_titles,
            clean_title_tags=clean_title_tags,
            normalize_subjects=normalize_subjects,
            max_subjects=max_subjects,
            enforce_modified_date=enforce_modified_date,
            enforce_single_identifier=enforce_single_identifier,
            clean_description_html=clean_description_html,
            normalize_marc_roles=normalize_marc_roles,
            verbose=verbose,
        )
        total += len(changes)
        if changes and not verbose:
            prefix = f"{epub_name} — " if epub_name else ""
            print(f"  Meta: {prefix}{len(changes)} change(s) in "
                  f"{os.path.basename(opf_path)}")
    return total


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — DRM DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def check_drm(input_path):
    try:
        with zipfile.ZipFile(input_path, 'r') as zf:
            names_lower = [n.lower() for n in zf.namelist()]
            if 'meta-inf/encryption.xml' in names_lower:
                try:
                    enc_index = names_lower.index('meta-inf/encryption.xml')
                    enc_data  = zf.read(zf.namelist()[enc_index]).decode('utf-8', errors='replace')
                    if 'http://ns.adobe.com/adept' in enc_data or 'urn:microsoft:DRM' in enc_data:
                        raise DRMProtectedError(
                            "DRM encryption detected (Adobe ADEPT or Microsoft DRM)")
                except DRMProtectedError: raise
                except Exception: raise DRMProtectedError("encryption.xml present but unreadable")
            if 'meta-inf/rights.xml' in names_lower:
                raise DRMProtectedError("Adobe rights.xml detected")
    except DRMProtectedError: raise
    except zipfile.BadZipFile: raise ValueError("Not a valid ZIP/EPUB file")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — ENCODING NORMALISATION
# ═══════════════════════════════════════════════════════════════════════════════

def normalise_encoding(temp_dir):
    if not HAS_CHARSET_NORMALIZER: return
    text_extensions = set(MINIFY_EXTENSIONS) | {'.txt', '.json'}
    for root, _, files in os.walk(temp_dir):
        for file in files:
            if os.path.splitext(file)[1].lower() not in text_extensions: continue
            path = os.path.join(root, file)
            try:
                raw = read_raw(path)
                # ── Fast-path: skip files that are already valid UTF-8 ────────
                # BOM-marked UTF-8 or pure ASCII (subset of UTF-8) need no
                # re-encoding; skip charset detection to avoid unnecessary work.
                if raw.startswith(b'\xef\xbb\xbf'):
                    # UTF-8 BOM — strip the BOM and write back as clean UTF-8
                    text = raw[3:].decode('utf-8', errors='replace')
                    write_text(path, text)
                    continue
                try:
                    raw.decode('utf-8')   # raises UnicodeDecodeError if not valid UTF-8
                    continue              # already valid UTF-8 — nothing to do
                except UnicodeDecodeError:
                    pass                  # needs detection + re-encoding
                result = detect_encoding(raw).best()
                if not result: continue
                enc  = str(result.encoding)
                text = raw.decode(enc, errors='replace')
                if os.path.splitext(file)[1].lower() == '.css':
                    # Strip any @charset declaration — we write UTF-8 always,
                    # so any @charset present is either redundant (UTF-8) or
                    # wrong (windows-1252 after re-encoding).  Do not re-add
                    # it: EPUB CSS is served without HTTP headers so readers
                    # use the XML declaration on the parent XHTML document.
                    text = re.sub(r'@charset\s+"[^"]*"\s*;\s*', '', text,
                                  flags=re.IGNORECASE)
                write_text(path, text)
            except Exception as e:
                _warn("encoding normalisation failed", path, e)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — BLOAT REMOVAL
# ═══════════════════════════════════════════════════════════════════════════════

def remove_bloat(temp_dir):
    # Remove known-bloat files and directories:
    # • OS artifacts: .DS_Store, Thumbs.db, desktop.ini
    # • Tool artifacts: Sigil config, page-map.xml, calibre bookmarks
    # • iTunes artifacts: iTunesMetadata.plist, iTunesArtwork
    # • Adobe .xpgt page-template files (dead format, v3)
    # Also strips inline <x-xpgt:matchingRule/> processing instructions and
    # adobe-xpgt CSS links from HTML files after removing the .xpgt files.
    xpgt_hrefs = set()  # collect hrefs of removed .xpgt files for HTML cleanup

    for root, dirs, files in os.walk(temp_dir, topdown=True):
        for d in list(dirs):
            if d.lower() in BLOAT_DIRNAMES:
                full = os.path.join(root, d)
                try:
                    shutil.rmtree(full)
                    dirs.remove(d)
                except Exception as e:
                    _warn("could not remove dir", full, e)
        for file in files:
            ext  = os.path.splitext(file)[1].lower()
            full = os.path.join(root, file)
            if file.lower() in BLOAT_FILENAMES or ext in BLOAT_EXTENSIONS:
                if ext in BLOAT_EXTENSIONS:
                    # Track relative path so we can scrub HTML references
                    xpgt_hrefs.add(os.path.basename(file))
                try:
                    os.remove(full)
                except Exception as e:
                    _warn("could not remove", full, e)

    # Scrub any remaining HTML references to .xpgt files
    if xpgt_hrefs: _scrub_xpgt_references(temp_dir, xpgt_hrefs)


def _scrub_xpgt_references(temp_dir, xpgt_hrefs):
    # Remove Adobe XPGT references from HTML/XHTML files:
    # • <?xml-stylesheet type="application/vnd.adobe-page-template+xml" href="*.xpgt"?>
    # • <link rel="stylesheet" type="application/vnd.adobe-page-template+xml" …/>
    # • <?xpgt … ?> processing instructions
    # and from the OPF manifest (removes the <item> entry for each .xpgt file).
    # Pattern for PI and link forms
    xpgt_pi_re   = re.compile(
        r'<\?xml-stylesheet[^?]*vnd\.adobe-page-template[^?]*\?>', re.IGNORECASE)
    xpgt_link_re = re.compile(
        r'<link\s[^>]*vnd\.adobe-page-template[^>]*/?>',  re.IGNORECASE)
    xpgt_proc_re = re.compile(r'<\?xpgt[^?]*\?>', re.IGNORECASE)

    for html_path in find_html(temp_dir):
        try:
            html = read_text(html_path)
            new  = xpgt_pi_re.sub('', html)
            new  = xpgt_link_re.sub('', new)
            new  = xpgt_proc_re.sub('', new)
            write_if_changed(html_path, html, new)
        except Exception as e:
            _warn("XPGT scrub failed", html_path, e)

    # Remove xpgt items from OPF manifest
    for opf_path in find_opf(temp_dir):
        try:
            content, changed = read_text(opf_path), False
            def remove_xpgt_item(m):
                nonlocal changed
                tag    = m.group(0)
                href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                if href_m and os.path.splitext(href_m.group(1))[1].lower() == '.xpgt':
                    changed = True
                    return ''
                return tag
            content = re.sub(r'<item\s[^>]*/\s*>', remove_xpgt_item, content, flags=re.IGNORECASE)
            if changed: write_text(opf_path, content)
        except Exception as e:
            _warn("XPGT OPF cleanup failed", opf_path, e)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — NAV / NCX
# ═══════════════════════════════════════════════════════════════════════════════

def cleanup_nav(temp_dir):
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            content   = read_text(opf_path)
            has_nav   = bool(re.search(
                r'<item\s[^>]*\bproperties="[^"]*\bnav\b[^"]*"', content, re.IGNORECASE))
            ncx_match = re.search(
                r'<item\s[^>]*\bmedia-type="application/x-dtbncx\+xml"[^>]*/?>',
                content, re.IGNORECASE)
            if has_nav and ncx_match:
                ncx_tag = ncx_match.group(0)
                href_m  = re.search(r'\bhref="([^"]+)"', ncx_tag, re.IGNORECASE)
                id_m    = re.search(r'\bid="([^"]+)"',   ncx_tag, re.IGNORECASE)
                if href_m:
                    ncx_file = os.path.normpath(os.path.join(opf_dir, href_m.group(1)))
                    if os.path.exists(ncx_file): os.remove(ncx_file)
                content = content.replace(ncx_tag, '')
                if id_m:
                    content = re.sub(
                        rf'<itemref\s[^>]*\bidref="{re.escape(id_m.group(1))}"[^>]*/?>',
                        '', content, flags=re.IGNORECASE)
                content = re.sub(r'\btoc="[^"]*"', '', content, flags=re.IGNORECASE)
                write_text(opf_path, content)
        except Exception as e:
            _warn("NCX/NAV cleanup failed", opf_path, e)


def _trim_depth_lines(lines, open_pat, close_pat, max_depth):
    # Remove entries deeper than max_depth using open_pat/close_pat to track depth.
    result, depth, skip_until = [], 0, None
    for line in lines:
        opens  = len(open_pat.findall(line))
        closes = len(close_pat.findall(line))
        if skip_until is None:
            for _ in range(opens):
                depth += 1
                if depth > max_depth:
                    skip_until = depth
                    break
            if skip_until is not None:
                for _ in range(closes): depth -= 1
                continue
            result.append(line)
            for _ in range(closes): depth -= 1
        else:
            for _ in range(opens):  depth += 1
            for _ in range(closes): depth -= 1
            if depth < skip_until:  skip_until = None
    return result


def _trim_ncx_depth(content, max_depth):
    lines = _trim_depth_lines(
        content.splitlines(keepends=True),
        re.compile(r'<navPoint\b', re.IGNORECASE),
        re.compile(r'</navPoint>', re.IGNORECASE),
        max_depth,
    )
    return ''.join(lines)

def _trim_nav_depth(content, max_depth):
    nav_m = re.search(
        r'(<nav\s[^>]*epub:type=["\']toc["\'][^>]*>)(.*?)(</nav>)',
        content, re.IGNORECASE | re.DOTALL)
    if not nav_m: return content
    nav_open, nav_body, nav_close = nav_m.group(1), nav_m.group(2), nav_m.group(3)
    trimmed = _trim_depth_lines(
        nav_body.splitlines(keepends=True),
        re.compile(r'<ol\b', re.IGNORECASE),
        re.compile(r'</ol>', re.IGNORECASE),
        max_depth,
    )
    return content[:nav_m.start()] + nav_open + ''.join(trimmed) + nav_close + content[nav_m.end():]

def _renumber_ncx_play_order(ncx_content: str) -> str:
    # Renumber playOrder attributes in an NCX sequentially from 1.
    # Required by DAISY spec: after depth-trimming or any navPoint removal,
    # gaps in playOrder values are invalid.
    counter = [0]
    def _replace_po(m):
        counter[0] += 1
        return f'{m.group(1)}"{counter[0]}"'
    return re.sub(r'(\bplayOrder=)"[^"]*"', _replace_po, ncx_content, flags=re.IGNORECASE)


def trim_toc_depth(temp_dir, max_depth=TOC_MAX_DEPTH):
    for ncx_path in find_ncx(temp_dir):
        try:
            trimmed = _trim_ncx_depth(read_text(ncx_path), max_depth)
            write_text(ncx_path, _renumber_ncx_play_order(trimmed))
        except Exception as e:
            _warn("NCX depth trim failed", ncx_path, e)
    for nav_path in find_nav(temp_dir):
        try:
            write_text(nav_path, _trim_nav_depth(read_text(nav_path), max_depth))
        except Exception as e:
            _warn("NAV depth trim failed", nav_path, e)


def _parse_ncx_navpoints(ncx_content):
    # Parse NCX navPoints into (label, href) pairs.
    # Returns [] if NCX is broken or empty.
    #
    # Two-pass approach (Bug #8 fix): first split on <navPoint> boundaries,
    # then extract label and href from each chunk individually.  This prevents
    # the single-regex approach from stitching a label from one navPoint with
    # an href from the next when closing tags are missing.
    entries = []
    try:
        # Split into per-navPoint chunks; each chunk ends just before the next
        # <navPoint or at end of string.
        chunks = re.split(r'(?=<navPoint\b)', ncx_content, flags=re.IGNORECASE)
        for chunk in chunks:
            if not re.match(r'\s*<navPoint\b', chunk, re.IGNORECASE): continue
            label_m = re.search(
                r'<navLabel[^>]*>\s*<text[^>]*>(.*?)</text>',
                chunk, re.IGNORECASE | re.DOTALL)
            href_m  = re.search(
                r'<content\s+src="([^"]+)"',
                chunk, re.IGNORECASE)
            if label_m and href_m:
                label = re.sub(r'<[^>]+>', '', label_m.group(1)).strip()
                href  = href_m.group(1).strip()
                if label and href: entries.append((label, href))
    except Exception: pass
    return entries


def _infer_language_from_html(temp_dir):
    # Scan spine HTML documents for a lang= or xml:lang= attribute on <html>
    # and return the most common non-empty value, normalised to BCP 47.
    #
    # NAV documents are excluded because the script's own generated NAV hardcodes
    # lang="{lang}" which would bootstrap `en` before the real language is found.
    # (Bug #10 fix)
    #
    # Returns a language string, or None if nothing usable is found.
    lang_pat  = re.compile(
        r'<html\b[^>]*\b(?:xml:)?lang="([^"]+)"', re.IGNORECASE)
    nav_paths = set(find_nav(temp_dir))  # exclude nav docs
    counts = {}
    for html_path in find_html(temp_dir):
        if html_path in nav_paths:      # skip nav docs
            continue
        try:
            m = lang_pat.search(read_text(html_path))
            if m:
                val = m.group(1).strip()
                if val: counts[val] = counts.get(val, 0) + 1
        except Exception: pass
    if not counts: return None
    best = max(counts, key=counts.get)
    return _normalize_language(best)


def _get_book_language(temp_dir):
    # Return the book's language tag in priority order:
    # 1. dc:language element in the OPF (authoritative).
    # 2. lang= / xml:lang= attribute on <html> elements (inferred).
    # 3. None (caller decides the final fallback).
    for opf_path in find_opf(temp_dir):
        try:
            m = re.search(r'<dc:language[^>]*>([^<]+)</dc:language>',
                          read_text(opf_path), re.IGNORECASE)
            if m: return m.group(1).strip()
        except Exception: pass
    # Fallback: infer from HTML content
    return _infer_language_from_html(temp_dir)


def _nav_entries_from_headings(opf_content, opf_dir, max_depth=2):
    # Scan spine documents for h1/h2 (and optionally h3) headings and build
    # flat TOC entries. Each entry is (label, href, depth) where depth is 1, 2,
    # or 3. Spine items with no headings are skipped entirely.
    #
    # Returns a list of (label, href, depth) tuples in spine order.
    entries = []
    heading_tags = [f'h{n}' for n in range(1, max_depth + 1)]
    heading_pat  = re.compile(
        r'<(h[1-' + str(max_depth) + r'])(?:\s[^>]*)?>([\s\S]*?)</\1>',
        re.IGNORECASE)
    id_pat = re.compile(r'\bid="([^"]+)"', re.IGNORECASE)

    for idref in re.findall(r'<itemref\s[^>]*\bidref="([^"]+)"', opf_content, re.IGNORECASE):
        href_m = re.search(
            rf'<item\s[^>]*\bid="{re.escape(idref)}"[^>]*\bhref="([^"]+)"',
            opf_content, re.IGNORECASE)
        if not href_m: continue
        href = href_m.group(1).split('#')[0]
        abs_path = os.path.normpath(os.path.join(opf_dir, href))
        if not os.path.exists(abs_path): continue
        if not abs_path.lower().endswith(('.html', '.xhtml', '.htm')): continue
        try:
            html = read_text(abs_path)
        except Exception: continue

        for m in heading_pat.finditer(html):
            tag_name = m.group(1).lower()
            depth    = int(tag_name[1])
            raw_text = re.sub(r'<[^>]+>', '', m.group(2)).strip()
            label    = re.sub(r'\s+', ' ', raw_text)
            if not label: continue
            # Use the heading element's own id as fragment if present
            id_m = id_pat.search(m.group(0))
            if id_m: entry_href = f'{href}#{id_m.group(1)}'
            else: entry_href = href
            entries.append((label, entry_href, depth))

    return entries


def _nav_entries_from_spine(opf_content, opf_dir):
    # Flat fallback: one entry per spine document that actually exists.
    # Spine items with no resolvable file are skipped.
    # Returns list of (label, href, depth=1).
    entries = []
    for idref in re.findall(r'<itemref\s[^>]*\bidref="([^"]+)"', opf_content, re.IGNORECASE):
        href_m = re.search(
            rf'<item\s[^>]*\bid="{re.escape(idref)}"[^>]*\bhref="([^"]+)"',
            opf_content, re.IGNORECASE)
        if not href_m: continue
        href     = href_m.group(1).split('#')[0]
        abs_path = os.path.normpath(os.path.join(opf_dir, href))
        if not os.path.exists(abs_path): continue
        if not abs_path.lower().endswith(('.html', '.xhtml', '.htm')): continue
        # Skip nav documents — they shouldn't appear in their own TOC
        try:
            doc_text = read_text(abs_path)
            if re.search(r'epub:type=["\']toc["\']', doc_text, re.IGNORECASE): continue
        except Exception: pass
        # No title text available — spine fallback skips titleless items per spec
        # (caller already decided to use this only when heading scan found nothing)
        entries.append((None, href, 1))  # None label → caller will skip

    # Return only entries that have a navigable file; label=None means skip
    return [(lbl, href, d) for lbl, href, d in entries if lbl is not None]


def _build_nav_xml(entries, lang):
    # Render (label, href, depth) entries as a flat EPUB3 NAV document.
    # depth=1 items render as plain <li>; depth=2 items get class="toc-h2";
    # depth=3 items get class="toc-h3" for visual indentation via CSS.
    # The flat <ol> never nests — one level, CSS provides indentation.
    lines = []
    for label, href, depth in entries:
        css_class = '' if depth == 1 else f' class="toc-h{depth}"'
        lines.append(f'      <li{css_class}><a href="{href}">{label}</a></li>')
    toc_items = '\n'.join(lines)

    # Minimal inline CSS so indentation is visible even without a linked stylesheet
    indent_css = (
        '      nav ol li.toc-h2 { margin-left: 1.5em; }\n'
        '      nav ol li.toc-h3 { margin-left: 3.0em; }\n'
    )

    return f'''<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:epub="http://www.idpf.org/2007/ops"
      lang="{lang}" xml:lang="{lang}">
<head>
  <meta charset="utf-8"/>
  <title>Table of Contents</title>
  <style>
{indent_css}  </style>
</head>
<body>
<nav epub:type="toc" id="toc">
  <h1>Table of Contents</h1>
  <ol>
{toc_items}
  </ol>
</nav>
</body>
</html>'''


def generate_epub3_nav(temp_dir):
    # Generate an EPUB3 NAV document for any OPF that lacks one.
    #
    # Strategy (in priority order):
    # 1. NCX exists and is parseable → use NCX navpoints (existing behaviour).
    # 2. NCX missing/broken → scan spine documents for h1/h2 headings
    # (depth controlled by TOC_HEADING_DEPTH config).
    # 3. Heading scan yields nothing → skip (no fabricated TOC).
    #
    # The generated NAV is a flat <ol> with CSS classes for visual indentation.
    # Spine items with no heading text are omitted.
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            opf = read_text(opf_path)

            # Skip if NAV already present
            if re.search(r'<item\s[^>]*\bproperties="[^"]*\bnav\b[^"]*"', opf, re.IGNORECASE):
                continue

            lang    = _get_book_language(temp_dir) or 'en'
            entries = []

            # ── Strategy 1: try NCX ──────────────────────────────────────────
            ncx_match = re.search(
                r'<item\s[^>]*\bmedia-type="application/x-dtbncx\+xml"[^>]*\bhref="([^"]+)"',
                opf, re.IGNORECASE)
            if ncx_match:
                ncx_path = os.path.normpath(
                    os.path.join(opf_dir, ncx_match.group(1)))
                if os.path.exists(ncx_path):
                    try:
                        ncx_entries = _parse_ncx_navpoints(read_text(ncx_path))
                        # Treat NCX as valid only if it produced entries
                        entries = [(lbl, href, 1) for lbl, href in ncx_entries]
                    except Exception: pass  # NCX unparseable → fall through to heading scan

            # ── Strategy 2: heading scan ─────────────────────────────────────
            if not entries:
                entries = _nav_entries_from_headings(
                    opf, opf_dir, max_depth=TOC_HEADING_DEPTH)

            # ── Strategy 3: nothing found → skip, never fabricate ────────────
            if not entries: continue

            nav_content = _build_nav_xml(entries, lang)
            nav_path    = os.path.join(opf_dir, 'nav.xhtml')
            write_text(nav_path, nav_content)

            nav_item = ('<item id="nav" href="nav.xhtml" '
                        'media-type="application/xhtml+xml" properties="nav"/>')
            opf = re.sub(r'(</manifest>)', f'  {nav_item}\n\\1', opf, flags=re.IGNORECASE)
            write_text(opf_path, opf)

        except Exception as e:
            print(f"\n  Warning: NAV generation failed for {opf_path}: {e}",
                  file=sys.stderr)


def _sync_nav_title_after_clean(temp_dir):
    # After metadata cleaning, update the <title> element in the NAV document
    # so it reflects the cleaned dc:title, not the pre-clean value baked in
    # when generate_epub3_nav() ran at step 2b.
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            opf   = read_text(opf_path)
            title_m = re.search(r'<dc:title[^>]*>([^<]+)</dc:title>', opf, re.IGNORECASE)
            if not title_m: continue
            clean_title = title_m.group(1).strip()
            if not clean_title: continue
            for nav_path in find_nav(temp_dir):
                try:
                    nav = read_text(nav_path)
                    new_nav = re.sub(
                        r'(<title>)[^<]*(</title>)',
                        rf'\g<1>{clean_title}\g<2>',
                        nav, count=1, flags=re.IGNORECASE)
                    write_if_changed(nav_path, nav, new_nav)
                except Exception: pass
        except Exception: pass


def _get_cover_spine_href(opf_content, opf_dir):
    # or None. Used by inject_landmarks to identify the cover page.
    spine_idrefs = re.findall(r'<itemref\s[^>]*\bidref="([^"]+)"', opf_content, re.IGNORECASE)
    if not spine_idrefs: return None
    first_idref  = spine_idrefs[0]
    first_href_m = re.search(
        rf'<item\s[^>]*\bid="{re.escape(first_idref)}"[^>]*\bhref="([^"]+)"',
        opf_content, re.IGNORECASE)
    if not first_href_m: return None
    first_path = os.path.normpath(os.path.join(opf_dir, first_href_m.group(1)))
    if not os.path.exists(first_path): return None
    try:
        html = read_text(first_path)
        if len(re.findall(r'<img\b[^>]*\bsrc="([^"]+)"', html, re.IGNORECASE)) == 1 \
                and len(re.sub(r'<[^>]+>', '', html).strip()) < 100:
            return first_href_m.group(1)
    except Exception: pass
    return None


def _infer_cover_image(opf_content, opf_dir):
    # Return the manifest item id of the cover image, or None if one already
    # exists or cannot be inferred.
    #
    # Cascade (stops at first match):
    # 1. <meta name="cover"> already present → already signalled, return None.
    # 2. <item properties="cover-image"> already present → return None.
    # 3. Manifest item whose id or href contains "cover" + is image/* → use it.
    # 4. Largest portrait-ratio image in the manifest → use it.
    #
    # Never fabricates a file. Returns the manifest item id to use, or None.
    # Already signalled — nothing to do
    if re.search(r'<meta\s[^>]*\bname="cover"', opf_content, re.IGNORECASE): return None
    if re.search(r'properties="[^"]*cover-image[^"]*"', opf_content, re.IGNORECASE): return None

    # Collect all image items: id → abs_path
    image_items = {}
    for m in re.finditer(
            r'<item\s[^>]*\bmedia-type="image/(?:jpeg|png|gif|webp)"[^>]*/\s*>',
            opf_content, re.IGNORECASE):
        tag     = m.group(0)
        id_m    = re.search(r'\bid="([^"]+)"',   tag, re.IGNORECASE)
        href_m  = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
        if id_m and href_m:
            abs_p = os.path.normpath(os.path.join(opf_dir, href_m.group(1)))
            if os.path.exists(abs_p): image_items[id_m.group(1)] = abs_p

    if not image_items: return None

    # Strategy 3: id or href contains "cover"
    for item_id, abs_p in image_items.items():
        basename = os.path.basename(abs_p).lower()
        if 'cover' in item_id.lower() or 'cover' in basename: return item_id

    # Strategy 4: largest portrait-ratio image
    if not HAS_PIL: return None
    best_id, best_px = None, 0
    for item_id, abs_p in image_items.items():
        try:
            with Image.open(abs_p) as img:
                w, h = img.size
                if h >= w and w * h > best_px:   # portrait or square only
                    best_id = item_id
                    best_px = w * h
        except Exception: pass
    return best_id


def inject_cover_metadata(temp_dir):
    # For any OPF that has no cover signals, infer the cover image and inject:
    # • <meta name="cover" content="item-id"/>  in <metadata>
    # • properties="cover-image" on the manifest item
    # Only runs when both signals are absent. Never overwrites existing signals.
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            opf      = read_text(opf_path)
            cover_id = _infer_cover_image(opf, opf_dir)
            if not cover_id: continue

            changed = False

            # Inject <meta name="cover"> into <metadata>
            meta_tag = f'<meta name="cover" content="{cover_id}"/>'
            opf = re.sub(r'(</metadata>)',
                         f'  {meta_tag}\n\\1', opf,
                         count=1, flags=re.IGNORECASE)
            changed = True

            # Add properties="cover-image" to the manifest item
            def add_cover_prop(m):
                tag = m.group(0)
                id_m = re.search(r'\bid="([^"]+)"', tag, re.IGNORECASE)
                if not id_m or id_m.group(1) != cover_id: return tag
                if 'cover-image' in tag: return tag
                prop_m = re.search(r'\bproperties="([^"]*)"', tag, re.IGNORECASE)
                if prop_m:
                    return tag.replace(prop_m.group(0),
                                       'properties="{} cover-image"'
                                       .format(prop_m.group(1)).strip())
                return re.sub(r'(/>|>)$', r' properties="cover-image"\1', tag.rstrip())

            opf = re.sub(r'<item\s[^>]*/\s*>', add_cover_prop, opf, flags=re.IGNORECASE)

            if changed: write_text(opf_path, opf)

        except Exception as e:
            print(f"\n  Warning: cover inference failed for {opf_path}: {e}",
                  file=sys.stderr)

def inject_landmarks(temp_dir):
    # EPUB3 structural semantics vocabulary for landmark types we can auto-detect.
    # Maps epub:type values found on spine document <body>/<section>/<div> elements
    # to their canonical landmark type and a human-readable label.
    _LANDMARK_TYPES = {
        'cover':            'Cover',
        'frontmatter':      'Front Matter',
        'foreword':         'Foreword',
        'preface':          'Preface',
        'introduction':     'Introduction',
        'bodymatter':       'Begin Reading',
        'chapter':          'Begin Reading',
        'part':             'Begin Reading',
        'conclusion':       'Conclusion',
        'afterword':        'Afterword',
        'appendix':         'Appendix',
        'bibliography':     'Bibliography',
        'index':            'Index',
        'glossary':         'Glossary',
        'copyright-page':   'Copyright',
        'colophon':         'Colophon',
        'toc':              'Table of Contents',
    }
    # Priority order for selecting the "begin reading" bodymatter landmark:
    # prefer chapter/part/bodymatter over all others
    _BODYMATTER_PRIORITY = ['chapter', 'part', 'bodymatter', 'introduction',
                            'foreword', 'preface', 'conclusion', 'afterword']

    nav_paths = find_nav(temp_dir)
    if not nav_paths: return
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            opf = read_text(opf_path)
            nav_item_m = re.search(
                r'<item\s[^>]*\bproperties="[^"]*\bnav\b[^"]*"[^>]*\bhref="([^"]+)"',
                opf, re.IGNORECASE)
            if not nav_item_m: continue
            nav_href = nav_item_m.group(1)
            nav_path = os.path.normpath(os.path.join(opf_dir, nav_href))
            if not os.path.exists(nav_path): continue
            nav_content = read_text(nav_path)
            if re.search(r'epub:type=["\']landmarks["\']', nav_content, re.IGNORECASE): continue

            # Build a map of spine hrefs → epub:type values detected in that document
            spine_idrefs = re.findall(r'<itemref\s[^>]*\bidref="([^"]+)"', opf, re.IGNORECASE)
            nav_filename = os.path.basename(nav_href)

            def _get_epub_types(href):

                abs_p = os.path.normpath(os.path.join(opf_dir, href))
                if not os.path.exists(abs_p): return set()
                try:
                    content = read_text(abs_p)
                    types = set()
                    for val in re.findall(
                            r'<(?:body|section|article|div)\b[^>]*\bepub:type="([^"]+)"',
                            content, re.IGNORECASE):
                        types.update(val.split())
                    return types
                except Exception: return set()

            # ── Collect landmarks ─────────────────────────────────────────────
            landmarks = [('toc', nav_href, 'Table of Contents')]
            cover_href = _get_cover_spine_href(opf, opf_dir)
            if cover_href:
                landmarks.append(('cover', cover_href, 'Cover'))

            # Walk spine to find other landmark-worthy documents
            bodymatter_href = None
            bodymatter_rank = len(_BODYMATTER_PRIORITY) + 1
            extra_landmarks = []   # (epub_type, href, label) for non-bodymatter entries

            for idref in spine_idrefs:
                href_m = re.search(
                    rf'<item\s[^>]*\bid="{re.escape(idref)}"[^>]*\bhref="([^"]+)"',
                    opf, re.IGNORECASE)
                if not href_m: continue
                href = href_m.group(1)
                if (cover_href and href == cover_href) or os.path.basename(href) == nav_filename:
                    continue

                types = _get_epub_types(href)
                if not types:
                    # No epub:type — use as bodymatter if we haven't found one yet
                    if bodymatter_href is None:
                        bodymatter_href = href
                    continue

                # Check for bodymatter-class types
                for priority, bm_type in enumerate(_BODYMATTER_PRIORITY):
                    if bm_type in types and priority < bodymatter_rank:
                        bodymatter_href = href
                        bodymatter_rank = priority
                        break

                # Check for specific non-bodymatter landmark types
                for lt in ('copyright-page', 'bibliography', 'index', 'glossary',
                           'appendix', 'colophon', 'afterword', 'conclusion'):
                    if lt in types:
                        label = _LANDMARK_TYPES.get(lt, lt.replace('-', ' ').title())
                        extra_landmarks.append((lt, href, label))
                        break

            if bodymatter_href:
                landmarks.append(('bodymatter', bodymatter_href, 'Begin Reading'))
            landmarks.extend(extra_landmarks)

            if not landmarks: continue
            items = '\n'.join(
                f'    <li><a epub:type="{lt}" href="{href}">{label}</a></li>'
                for lt, href, label in landmarks)
            landmarks_nav = (
                f'\n<nav epub:type="landmarks" id="landmarks" hidden="">\n'
                f'  <h2>Landmarks</h2>\n  <ol>\n{items}\n  </ol>\n</nav>\n'
            )
            nav_content = re.sub(
                r'(</body>)', landmarks_nav + r'\1', nav_content, flags=re.IGNORECASE)
            write_text(nav_path, nav_content)
        except Exception as e:
            _warn("landmark injection failed", opf_path, e)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — SPINE / MANIFEST CLEANUP
# ═══════════════════════════════════════════════════════════════════════════════

def remove_duplicate_spine_items(temp_dir):
    for opf_path in find_opf(temp_dir):
        try:
            content  = read_text(opf_path)
            seen_ids = set()
            def dedup_itemref(match):
                tag   = match.group(0)
                idref = re.search(r'\bidref="([^"]+)"', tag, re.IGNORECASE)
                if not idref: return tag
                val = idref.group(1).lower()
                if val in seen_ids: return ''
                seen_ids.add(val)
                return tag
            content = re.sub(r'<itemref\s[^>]*/>', dedup_itemref, content, flags=re.IGNORECASE)
            write_text(opf_path, content)
        except Exception as e:
            _warn("spine dedup failed", opf_path, e)


def _parse_manifest_items(opf_content):
    # Single-pass extraction: handles both attribute orderings (id first or href first)
    # and both self-closing and paired tags. Eliminates duplicate work and divergence
    # risk of the previous triple-regex approach.
    id_to_href = {}
    for m in re.finditer(r'<item\s[^>]*>', opf_content, re.IGNORECASE):
        tag    = m.group(0)
        id_m   = re.search(r'\bid="([^"]+)"',   tag, re.IGNORECASE)
        href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
        if id_m and href_m:
            id_to_href[id_m.group(1)] = href_m.group(1)
    return id_to_href


def cleanup_manifest(temp_dir):
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            content     = read_text(opf_path)
            id_to_href  = _parse_manifest_items(content)
            referenced  = set()
            for href in id_to_href.values():
                referenced.add(os.path.normpath(os.path.join(opf_dir, href.split('#')[0])))
            for idref in re.findall(r'<itemref\s[^>]*\bidref="([^"]+)"', content, re.IGNORECASE):
                if idref in id_to_href:
                    referenced.add(os.path.normpath(
                        os.path.join(opf_dir, id_to_href[idref].split('#')[0])))
            for html_abs in list(referenced):
                if not html_abs.lower().endswith(('.html', '.xhtml', '.htm')): continue
                if not os.path.exists(html_abs): continue
                try:
                    html_dir = os.path.dirname(html_abs)
                    for sub_href in re.findall(r'(?:src|href)="([^"#][^"]*)"',
                                               read_text(html_abs), re.IGNORECASE):
                        referenced.add(os.path.normpath(
                            os.path.join(html_dir, sub_href.split('#')[0])))
                except Exception: pass
            referenced.add(opf_path)
            # Explicitly protect NAV files — they are manifest items referenced
            # via OPF properties="nav", not via HTML src= attrs, so they would
            # be incorrectly deleted as "orphans" without this guard.
            for nav_path in find_nav(temp_dir):
                referenced.add(os.path.normpath(nav_path))
            protected_dirs = {os.path.normpath(os.path.join(temp_dir, 'META-INF'))}
            for root, _, files in os.walk(temp_dir):
                if any(root.startswith(p) for p in protected_dirs): continue
                for file in files:
                    full = os.path.normpath(os.path.join(root, file))
                    if (full == os.path.normpath(os.path.join(temp_dir, 'mimetype'))
                            or file.lower().endswith('.opf')
                            or full in referenced):
                        continue
                    try:
                        os.remove(full)
                    except Exception as e:
                        _warn("could not remove orphan", full, e)

            def check_item(match):
                tag    = match.group(0)
                href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                if not href_m: return tag
                return tag if os.path.exists(
                    os.path.normpath(os.path.join(opf_dir, href_m.group(1)))) else ''

            new_content = re.sub(r'<item\s[^>]*/\s*>', check_item, content, flags=re.IGNORECASE)
            new_content = re.sub(
                r'<item\s[^>]*>(?:</item>)?', check_item, new_content, flags=re.IGNORECASE)
            write_if_changed(opf_path, content, new_content)
        except Exception as e:
            _warn("manifest cleanup failed", opf_path, e)


def split_large_xhtml(temp_dir):
    # Split any XHTML/HTML spine document larger than XHTML_SPLIT_KB into
    # smaller parts, splitting at h1/h2/h3 heading boundaries.
    #
    # Why: Kindle has an internal ~300 KB limit per spine item and silently
    # truncates or fails on larger files.  Large monolithic files also degrade
    # page-turn performance on Kobo and older Apple Books.
    #
    # Strategy:
    # 1. Find all spine documents larger than XHTML_SPLIT_KB.
    # 2. For each, locate every h1/h2/h3 opening tag in the body — these are
    # the natural chapter/section boundaries.
    # 3. Split the body content into segments at those boundaries.  The first
    # segment contains everything before the first heading (preamble) plus
    # the first heading onward; each subsequent segment starts at the next
    # heading.  Segments that would be under 10 KB are merged with the
    # next one to avoid creating trivially small files.
    # 4. Write each segment as {stem}_part01.xhtml, {stem}_part02.xhtml, …
    # Each file gets the original <head> block verbatim.
    # 5. Update the OPF manifest and spine: remove the original item/itemref,
    # insert new items and itemrefs at the same spine position.
    # 6. Update cross-document fragment links in all other HTML, NCX, and NAV
    # files: href="old.xhtml#id" → href="partNN.xhtml#id" using a mapping
    # built from which part file contains which id= value.
    # 7. Delete the original file.
    #
    # Files with no h1/h2/h3 headings are left untouched even if oversized
    # (no safe split point).  Files already below XHTML_SPLIT_KB are skipped.
    for opf_path in find_opf(temp_dir):
        opf_dir     = os.path.dirname(opf_path)
        opf_content = read_text(opf_path)

        # ── Build spine order from OPF ────────────────────────────────────────
        # We need the ordered list of spine idrefs so we can insert parts at
        # the correct position.
        spine_idrefs = re.findall(
            r'<itemref\s[^>]*\bidref="([^"]+)"[^>]*/?>',
            opf_content, re.IGNORECASE)

        # id → href mapping from manifest (reuse shared parser — no divergence risk)
        id_to_href = _parse_manifest_items(opf_content)

        href_to_id = {v: k for k, v in id_to_href.items()}

        # Collect all HTML spine files with their absolute paths
        spine_files = []
        for idref in spine_idrefs:
            href = id_to_href.get(idref)
            if not href: continue
            abs_path = os.path.normpath(os.path.join(opf_dir, href))
            if (os.path.exists(abs_path) and
                    abs_path.lower().endswith(('.html', '.xhtml', '.htm'))):
                spine_files.append((idref, href, abs_path))

        # ── Process each oversized file ───────────────────────────────────────
        # Track all renames for cross-document link updates
        # Maps: old_basename → {fragment_id → new_basename}
        fragment_remap = {}   # {old_abs_path: {frag_id: new_abs_path}}

        for orig_idref, orig_href, orig_abs in spine_files:
            size_kb = os.path.getsize(orig_abs) / 1024
            if size_kb < XHTML_SPLIT_KB: continue

            try:
                content = read_text(orig_abs)
            except Exception as e:
                print(f"\n  Warning: split_large_xhtml: cannot read {orig_abs}: {e}",
                      file=sys.stderr)
                continue

            # ── Extract head block and body content ───────────────────────────
            head_m = re.search(r'(<head\b[^>]*>.*?</head>)', content,
                               re.DOTALL | re.IGNORECASE)
            body_m = re.search(r'<body\b([^>]*)>(.*?)</body>',  content,
                               re.DOTALL | re.IGNORECASE)
            if not body_m: continue   # malformed — skip

            head_block  = head_m.group(0) if head_m else '<head></head>'
            body_attrs  = body_m.group(1)   # e.g. class="calibre"
            body_content = body_m.group(2)

            # ── Find heading split points in body content ─────────────────────
            # Match the opening tag of any h1/h2/h3
            heading_pat = re.compile(r'<h[123]\b[^>]*>', re.IGNORECASE)
            heading_positions = [m.start() for m in heading_pat.finditer(body_content)]

            if not heading_positions: continue  # no safe split point

            # Build segments: each starts at a heading position.
            # If there's content before the first heading, it becomes a preamble
            # that is attached to the first segment (not a standalone file).
            raw_segments = []
            for i, pos in enumerate(heading_positions):
                end = (heading_positions[i + 1]
                       if i + 1 < len(heading_positions) else len(body_content))
                raw_segments.append(body_content[pos:end])

            # Prepend any preamble (content before first heading) to segment 0
            preamble = body_content[:heading_positions[0]]
            if preamble.strip(): raw_segments[0] = preamble + raw_segments[0]

            # Merge segments that are too small (< 10 KB) into the next one
            MIN_PART_KB = 10
            segments = []
            pending  = ''
            for seg in raw_segments:
                pending += seg
                if len(pending.encode('utf-8')) >= MIN_PART_KB * 1024:
                    segments.append(pending)
                    pending = ''
            if pending:
                if segments: segments[-1] += pending   # merge tail into last segment
                else: segments.append(pending)  # only one segment after merge

            if len(segments) < 2: continue   # splitting would produce only one part — skip

            # ── Derive XML declaration and html element from original ──────────
            xml_decl_m = re.match(r'<\?xml[^?]*\?>\s*', content)
            xml_decl   = xml_decl_m.group(0) if xml_decl_m else ''
            html_open_m = re.search(r'<html\b[^>]*>', content, re.IGNORECASE)
            html_open = (html_open_m.group(0) if html_open_m
                         else '<html xmlns="http://www.w3.org/1999/xhtml">')

            # ── Write part files ──────────────────────────────────────────────
            orig_file_dir = os.path.dirname(orig_abs)
            stem          = os.path.splitext(os.path.basename(orig_abs))[0]
            ext           = os.path.splitext(os.path.basename(orig_abs))[1] or '.xhtml'

            part_abs_paths  = []
            part_hrefs      = []   # relative to OPF dir
            part_ids        = []   # manifest ids

            for idx, seg in enumerate(segments, start=1):
                part_filename = f"{stem}_part{idx:02d}{ext}"
                part_abs      = os.path.join(orig_file_dir, part_filename)
                # href relative to OPF dir (same as original, just different filename)
                orig_href_dir = os.path.dirname(orig_href)
                part_href     = (f"{orig_href_dir}/{part_filename}".lstrip('/')
                                 if orig_href_dir else part_filename)

                part_content = (
                    xml_decl +
                    html_open + '\n' +
                    head_block + '\n' +
                    f'<body{body_attrs}>\n' +
                    seg +
                    '\n</body>\n</html>'
                )
                try:
                    write_text(part_abs, part_content)
                except Exception as e:
                    print(f"\n  Warning: split_large_xhtml: cannot write {part_abs}: {e}",
                          file=sys.stderr)
                    # Clean up any parts already written and abort this file
                    for p in part_abs_paths:
                        try: os.remove(p)
                        except Exception: pass
                    part_abs_paths = []
                    break

                part_abs_paths.append(part_abs)
                part_hrefs.append(part_href)

                # Build manifest id: sanitise filename → valid XML id
                raw_id = re.sub(r'[^a-zA-Z0-9_-]', '_', os.path.splitext(part_filename)[0])
                # Ensure it starts with a letter (XML id requirement)
                if raw_id and raw_id[0].isdigit(): raw_id = 'x' + raw_id
                part_ids.append(raw_id)

            if not part_abs_paths or len(part_abs_paths) != len(segments):
                continue   # write failed partway through

            # ── Build fragment → part-file mapping ────────────────────────────
            id_pat = re.compile(r'\bid="([^"]+)"', re.IGNORECASE)
            frag_map = {}   # fragment_id → part_abs_path
            for part_abs, part_content_text in zip(
                    part_abs_paths,
                    [read_text(p) for p in part_abs_paths]):
                for m in id_pat.finditer(part_content_text): frag_map[m.group(1)] = part_abs
            fragment_remap[orig_abs] = frag_map

            # ── Update OPF manifest and spine ─────────────────────────────────
            try:
                opf_content = read_text(opf_path)

                # 1. Add new manifest items before </manifest>
                new_items = ''
                for pid, phref in zip(part_ids, part_hrefs):
                    # Skip if id already exists (idempotency)
                    if f'id="{pid}"' not in opf_content:
                        new_items += (
                            f'\n    <item id="{pid}" href="{phref}"'
                            f' media-type="application/xhtml+xml"/>'
                        )
                if new_items:
                    opf_content = re.sub(
                        r'([ \t]*</manifest>)',
                        new_items + r'\n\1',
                        opf_content, count=1, flags=re.IGNORECASE)

                # 2. Replace original spine itemref with one per part
                new_itemrefs = ''.join(
                    f'<itemref idref="{pid}"/>' for pid in part_ids)

                def replace_spine_item(m):
                    tag   = m.group(0)
                    idref = re.search(r'\bidref="([^"]+)"', tag, re.IGNORECASE)
                    if idref and idref.group(1) == orig_idref: return new_itemrefs
                    return tag

                opf_content = re.sub(
                    r'<itemref\s[^>]*/?>',
                    replace_spine_item,
                    opf_content, flags=re.IGNORECASE)

                # 3. Remove original manifest item
                opf_content = re.sub(
                    r'<item\s[^>]*\bid="' + re.escape(orig_idref) + r'"[^>]*/?>',
                    '', opf_content, flags=re.IGNORECASE)
                # Also catch reversed attribute order
                opf_content = re.sub(
                    r'<item\s[^>]*\bhref="' + re.escape(orig_href) + r'"[^>]*/?>',
                    '', opf_content, flags=re.IGNORECASE)

                write_text(opf_path, opf_content)
            except Exception as e:
                print(f"\n  Warning: split_large_xhtml: OPF update failed for {orig_abs}: {e}",
                      file=sys.stderr)
                continue

            # ── Delete original file ──────────────────────────────────────────
            try:
                os.remove(orig_abs)
            except Exception as e:
                print(f"\n  Warning: split_large_xhtml: cannot remove {orig_abs}: {e}",
                      file=sys.stderr)

            print(f"  Note: split {os.path.basename(orig_abs)} ({size_kb:.0f} KB)"
                  f" into {len(segments)} parts", file=sys.stderr)

        # ── Update cross-document links in all other HTML/NCX/NAV files ───────
        if not fragment_remap: return

        all_html = find_html(temp_dir)
        ncx_files = find_ncx(temp_dir)
        nav_files = find_nav(temp_dir)
        files_to_update = set(all_html + ncx_files + nav_files)

        for update_path in files_to_update:
            # Skip the newly-created part files themselves
            abs_norm = os.path.normpath(update_path)
            if any(abs_norm == os.path.normpath(p)
                   for pmap in fragment_remap.values()
                   for p in pmap.values()):
                continue
            try:
                text    = read_text(update_path)
                changed = False
                update_dir = os.path.dirname(update_path)

                def remap_href(m):
                    nonlocal changed
                    raw = m.group(1)
                    # Leave external links alone
                    if raw.startswith(('http://', 'https://', 'mailto:')): return m.group(0)
                    if '#' not in raw: return m.group(0)
                    path_part, fragment = raw.split('#', 1)
                    if not path_part: return m.group(0)   # same-document fragment

                    # Resolve to absolute path
                    target_abs = os.path.normpath(
                        os.path.join(update_dir, path_part))

                    if target_abs not in fragment_remap:
                        return m.group(0)   # not one of the split files

                    frag_map = fragment_remap[target_abs]
                    if fragment not in frag_map:
                        # Fragment may be in part01 (beginning of file) —
                        # if no mapping found, point to first part
                        if frag_map:
                            first_part = list(frag_map.values())[0]
                            new_rel = os.path.relpath(first_part, update_dir)
                            new_rel = new_rel.replace(os.sep, '/')
                            changed = True
                            return f'href="{new_rel}#{fragment}"'
                        return m.group(0)

                    new_abs = frag_map[fragment]
                    new_rel = os.path.relpath(new_abs, update_dir)
                    new_rel = new_rel.replace(os.sep, '/')
                    changed = True
                    return f'href="{new_rel}#{fragment}"'

                new_text = re.sub(r'\bhref="([^"]*)"', remap_href, text,
                                  flags=re.IGNORECASE)
                # NCX files use <content src="file#fragment"> not href=
                new_text = re.sub(r'\bsrc="([^"]*)"', remap_href, new_text,
                                  flags=re.IGNORECASE)
                if changed: write_text(update_path, new_text)
            except Exception as e:
                print(f"\n  Warning: split_large_xhtml: link update failed"
                      f" for {update_path}: {e}", file=sys.stderr)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — FONTS
# ═══════════════════════════════════════════════════════════════════════════════

def deduplicate_fonts(temp_dir):
    opf_paths, css_paths, font_files = find_opf(temp_dir), find_css(temp_dir), []
    for root, _, files in os.walk(temp_dir):
        for f in files:
            if os.path.splitext(f)[1].lower() in FONT_EXTENSIONS:
                font_files.append(os.path.join(root, f))
    if len(font_files) < 2: return
    hash_map = {}
    for path in font_files:
        try:
            h = hashlib.sha256(read_raw(path)).hexdigest()
            hash_map.setdefault(h, []).append(path)
        except Exception: pass
    for paths in hash_map.values():
        if len(paths) < 2: continue
        canonical = paths[0]
        for dup in paths[1:]:
            dup_base = os.path.basename(dup)
            try:
                os.remove(dup)
            except Exception: continue
            for css_path in css_paths:
                try:
                    css = read_text(css_path)
                    if dup_base in css:
                        rel = os.path.relpath(canonical, os.path.dirname(css_path))
                        write_text(css_path, css.replace(dup_base, rel.replace(os.sep, '/')))
                except Exception: pass
            for opf_path in opf_paths:
                try:
                    opf = read_text(opf_path)
                    if dup_base in opf:
                        rel = os.path.relpath(canonical, os.path.dirname(opf_path))
                        write_text(opf_path, opf.replace(dup_base, rel.replace(os.sep, '/')))
                except Exception: pass


def validate_font_integrity(temp_dir):
    # Feature G: Cross-check every @font-face src: URL in CSS against actual files
    # on disk and against the OPF manifest.
    #
    # Actions:
    # • Remove @font-face blocks whose src file does not exist on disk.
    # • Log a warning for manifest font items missing from CSS @font-face.
    # • Correct MIME types for font manifest items if wrong
    # (font/ttf, font/otf, font/woff, font/woff2).
    #
    # This runs after deduplicate_fonts so canonical paths are already settled.
    FONT_MIME = {
        '.ttf': 'font/ttf', '.otf': 'font/otf',
        '.woff': 'font/woff', '.woff2': 'font/woff2',
    }
    # Also accept legacy MIME types that are in the wild
    LEGACY_TO_CORRECT = {
        'application/x-font-ttf': 'font/ttf',
        'application/x-font-otf': 'font/otf',
        'application/font-woff':  'font/woff',
    }

    for css_path in find_css(temp_dir):
        css_dir = os.path.dirname(css_path)
        try:
            css = read_text(css_path)
            changed = False

            def validate_font_face(m):
                nonlocal changed
                block = m.group(0)
                # Extract src: URL(s) from this @font-face block
                src_urls = re.findall(
                    r'src\s*:[^;{]*url\(["\']?([^"\')\s]+)["\']?\)',
                    block, re.IGNORECASE)
                if not src_urls: return block  # no src — leave alone
                # Check each src URL; if ALL are missing, remove the block
                found_any = False
                for url in src_urls:
                    if url.startswith(('http://', 'https://', 'data:')):
                        found_any = True
                        break
                    font_path = os.path.normpath(os.path.join(css_dir, url.split('?')[0]))
                    if os.path.exists(font_path):
                        found_any = True
                        break
                if not found_any:
                    print(f"\n  Warning: removing @font-face with missing src "
                          f"in {os.path.basename(css_path)}: {src_urls[0]}", file=sys.stderr)
                    changed = True
                    return '/* @font-face removed: font file not found */'
                return block

            css = re.sub(r'@font-face\s*\{[^}]*\}', validate_font_face,
                         css, flags=re.IGNORECASE | re.DOTALL)
            if changed: write_text(css_path, css)
        except Exception as e:
            _warn("font integrity check failed", css_path, e)

    # Correct legacy font MIME types in OPF manifest
    for opf_path in find_opf(temp_dir):
        try:
            content, changed = read_text(opf_path), False
            def fix_font_mime(m):
                nonlocal changed
                tag  = m.group(0)
                mt_m = re.search(r'\bmedia-type="([^"]+)"', tag, re.IGNORECASE)
                if not mt_m: return tag
                mt = mt_m.group(1).lower()
                if mt in LEGACY_TO_CORRECT:
                    correct = LEGACY_TO_CORRECT[mt]
                    changed = True
                    return tag.replace(mt_m.group(0), f'media-type="{correct}"')
                # Also correct by extension if MIME is wrong
                href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                if href_m:
                    ext = os.path.splitext(href_m.group(1))[1].lower()
                    expected = FONT_MIME.get(ext)
                    if expected and mt != expected:
                        changed = True
                        return tag.replace(mt_m.group(0), f'media-type="{expected}"')
                return tag
            content = re.sub(r'<item\s[^>]*/\s*>', fix_font_mime, content, flags=re.IGNORECASE)
            if changed: write_text(opf_path, content)
        except Exception as e:
            _warn("font MIME repair failed", opf_path, e)


def _collect_epub_codepoints(temp_dir):
    # Scan all HTML and CSS files in the EPUB and return the set of Unicode
    # codepoints that actually appear in the book's text and styled content.
    #
    # Sources:
    # • HTML text nodes  — everything outside of tags (strip tags, keep text)
    # • CSS content:     — pseudo-element text values (::before / ::after)
    # • CSS hex escapes  — \\2022 style escape sequences in content: values
    #
    # Always includes the full printable ASCII range (U+0020–U+007E) as a
    # safety baseline.  These 95 characters add negligible bytes to any font
    # but prevent a subset font being unusable if a previously-unseen ASCII
    # char appears (e.g. in reader UI injected text).
    codepoints = set()

    # ── Always-keep: printable ASCII ─────────────────────────────────────────
    codepoints.update(range(0x20, 0x7F))   # space through tilde

    _tag_re    = re.compile(r'<[^>]+>')
    _escape_re = re.compile(r'\\([0-9a-fA-F]{4,6})')
    _content_re = re.compile(
        r'content\s*:\s*["\']([^"\']*)["\']', re.IGNORECASE)

    # ── HTML files ────────────────────────────────────────────────────────────
    for html_path in find_html(temp_dir):
        try:
            text = read_text(html_path)
            # Strip all tags to get only text content
            stripped = _tag_re.sub(' ', text)
            codepoints.update(filter(lambda cp: cp > 0x1F, map(ord, stripped)))
            # Also grab any CSS content: values embedded in style= attributes
            for val in _content_re.findall(text):
                codepoints.update(filter(lambda cp: cp > 0x1F, map(ord, val)))
                for hex_val in _escape_re.findall(val):
                    try:
                        codepoints.add(int(hex_val, 16))
                    except ValueError: pass
        except Exception: pass

    # ── CSS files ─────────────────────────────────────────────────────────────
    for css_path in find_css(temp_dir):
        try:
            css = read_text(css_path)
            # content: 'text' or content: "text"
            for val in _content_re.findall(css):
                codepoints.update(filter(lambda cp: cp > 0x1F, map(ord, val)))
                # CSS hex escapes inside content values
                for hex_val in _escape_re.findall(val):
                    try:
                        codepoints.add(int(hex_val, 16))
                    except ValueError: pass
            # Also catch bare CSS escapes anywhere in the file (outside quotes)
            for hex_val in _escape_re.findall(css):
                try:
                    codepoints.add(int(hex_val, 16))
                except ValueError: pass
        except Exception: pass

    # Remove control characters and surrogates — not valid font codepoints
    codepoints -= set(range(0x00, 0x20))
    codepoints -= set(range(0xD800, 0xE000))  # surrogate range
    codepoints.discard(0xFFFE)
    codepoints.discard(0xFFFF)

    return codepoints


def subset_fonts(temp_dir):
    # Subset every embedded font larger than FONT_SUBSET_MIN_KB to the
    # Unicode codepoints actually used in the book's text and CSS content.
    #
    # Why: Embedded fonts in EPUBs frequently contain the full character
    # set for the language (Latin: ~800–1200 glyphs, CJK: 20 000+), but
    # most books use only a few hundred distinct characters.  Subsetting
    # to used glyphs only typically reduces font file size by 50–90%.
    #
    # Requires fontTools (soft dependency, like Pillow).  Silently skips
    # if fontTools is not installed.  woff2 is skipped if brotli is not
    # installed (fontTools cannot decode or re-encode woff2 without it).
    #
    # Per-font safety:
    # • ignore_missing_glyphs=True  — never raises on missing chars
    # • hinting=False               — strip hinting (e-readers ignore it)
    # • Full printable ASCII always kept as a safety baseline
    # • Any font that raises during subset is left completely untouched
    if not HAS_FONTTOOLS: return

    # Collect codepoints used across the entire EPUB
    used_codepoints = _collect_epub_codepoints(temp_dir)
    if not used_codepoints: return

    # Find all font files
    font_files = []
    for root, _, files in os.walk(temp_dir):
        for fname in files:
            ext = os.path.splitext(fname)[1].lower()
            if ext in FONT_EXTENSIONS: font_files.append(os.path.join(root, fname))

    if not font_files: return

    from fontTools.subset import Subsetter, Options
    from fontTools.ttLib import TTFont

    subsetted = 0
    for font_path in font_files:
        ext      = os.path.splitext(font_path)[1].lower()
        size_kb  = os.path.getsize(font_path) / 1024

        if size_kb < FONT_SUBSET_MIN_KB: continue   # too small to bother

        if ext == '.woff2' and not HAS_BROTLI:
            # Cannot decode woff2 without brotli — leave untouched
            print(f"\n  Note: skipping woff2 subset (brotli not installed):"
                  f" {os.path.basename(font_path)}", file=sys.stderr)
            continue

        try:
            tt = TTFont(font_path)

            # Check the font has a usable cmap — if not, skip
            cmap = tt.getBestCmap()
            if not cmap:
                tt.close()
                continue

            # Only keep codepoints that are actually in this font's cmap
            font_codepoints = set(cmap.keys())
            subset_cps = list(used_codepoints & font_codepoints)

            if not subset_cps:
                tt.close()
                continue

            # Build subsetter
            opts = Options()
            opts.ignore_missing_glyphs = True
            opts.hinting               = False   # e-readers don't use hinting
            opts.desubroutinize        = True    # helps CFF/OTF size

            subsetter = Subsetter(options=opts)
            subsetter.populate(unicodes=subset_cps)
            subsetter.subset(tt)

            # Save back to the same path (preserves woff/woff2 flavor)
            tt.save(font_path)
            tt.close()

            size_after = os.path.getsize(font_path) / 1024
            saving_pct = (1 - size_after / size_kb) * 100
            print(f"\n  Note: subset {os.path.basename(font_path)}"
                  f" {size_kb:.0f}→{size_after:.0f} KB ({saving_pct:.0f}% smaller)",
                  file=sys.stderr)
            subsetted += 1

        except Exception as e:
            print(f"\n  Warning: font subset failed for"
                  f" {os.path.basename(font_path)}: {e}", file=sys.stderr)
            # Font left untouched — the original bytes are still on disk
            # since we only called tt.save() after a successful subset


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — CSS
# ═══════════════════════════════════════════════════════════════════════════════

def _flatten_selector(selector):
    selector = re.sub(r'^(html|body)\s+', '', selector.strip(), flags=re.IGNORECASE)
    selector = re.sub(r'^(div|section|article|main)\s+(?=[a-z#.\[])',
                      '', selector.strip(), flags=re.IGNORECASE)
    return selector.strip()

def _simplify_css_block(css_content):
    result, i, n = [], 0, len(css_content)
    while i < n:
        while i < n and css_content[i].isspace(): i += 1
        if i >= n: break
        brace_pos = css_content.find('{', i)
        if brace_pos == -1: result.append(css_content[i:]); break
        prelude  = css_content[i:brace_pos].strip()
        depth, j = 0, brace_pos
        while j < n:
            if css_content[j] == '{':   depth += 1
            elif css_content[j] == '}':
                depth -= 1
                if depth == 0: break
            j += 1
        block_body = css_content[brace_pos + 1:j]
        i = j + 1
        if prelude.startswith('@'):
            # Recurse into @media/@supports bodies so nested rules are also
            # deduplicated.  @keyframes and @font-face are single-level and
            # safe to recurse too (their bodies contain no sub-selectors).
            result.append(prelude + '{' + _simplify_css_block(block_body) + '}')
            continue
        unique_sel, seen_sel = [], set()
        for s in [_flatten_selector(s) for s in prelude.split(',') if s.strip()]:
            if s and s not in seen_sel: seen_sel.add(s); unique_sel.append(s)
        seen_props = {}
        for decl in [d.strip() for d in block_body.split(';') if d.strip()]:
            prop = decl.split(':')[0].strip().lower() if ':' in decl else decl
            seen_props[prop] = decl
        result.append(', '.join(unique_sel) + '{' + ';'.join(seen_props.values()) + '}')
    return '\n'.join(result)

def apply_css_transforms(temp_dir,
                         do_reduce_complexity=True,
                         do_strip_important=True,
                         do_inject_hyphenation=True,
                         do_inject_widows=True,
                         do_inject_page_break=True,
                         do_font_display_swap=True,
                         do_strip_unsafe=True,
                         kindle_mode=False,
                         do_convert_font_units=False,
                         do_strip_bg=False):
    # Single-pass CSS transform: reads each CSS file once, applies all active
    # sub-transforms in sequence, writes back only if content changed.
    # Each sub-transform has its own try/except so one failure cannot abort
    # the remaining transforms for that file.
    _IMP_RE = re.compile(r'\s*!important\b', re.IGNORECASE)
    _PBK = (
        '\n/* page-break hints (injected by epub_toolkit) */\n'
        'figure, table, blockquote, pre, aside{\n'
        '  page-break-inside:avoid;\n  break-inside:avoid;\n}\n'
        'h1, h2, h3, h4, h5, h6{\n'
        '  page-break-after:avoid;\n  break-after:avoid;\n'
        '  page-break-inside:avoid;\n  break-inside:avoid;\n}\n'
    )

    for css_path in find_css(temp_dir):
        try:
            original = read_text(css_path)
            css = original

            # 1. Reduce CSS complexity (strip comments, flatten selectors)
            if do_reduce_complexity:
                try:
                    css = _simplify_css_block(
                        re.sub(r'/\*.*?\*/', '', css, flags=re.DOTALL))
                except Exception as e:
                    print(f"\n  Warning: CSS complexity reduction failed for "
                          f"{css_path}: {e}", file=sys.stderr)

            # 2. Strip !important
            if do_strip_important:
                try:
                    css = _IMP_RE.sub('', css)
                except Exception as e:
                    print(f"\n  Warning: !important stripping failed for "
                          f"{css_path}: {e}", file=sys.stderr)

            # 3. Inject hyphenation into body{}
            if do_inject_hyphenation:
                try:
                    if not re.search(r'\bhyphens\s*:', css, re.IGNORECASE):
                        def _hyph(m):
                            return re.sub(r'\}(\s*)$',
                                          r';hyphens:auto;adobe-hyphenate:auto;}\1',
                                          m.group(0))
                        css, n = re.subn(r'\bbody\s*\{[^}]*\}', _hyph, css,
                                         flags=re.IGNORECASE | re.DOTALL)
                        if n == 0:
                            css += '\nbody{hyphens:auto;adobe-hyphenate:auto;}\n'
                except Exception as e:
                    print(f"\n  Warning: hyphenation CSS injection failed for "
                          f"{css_path}: {e}", file=sys.stderr)

            # 4. Inject widows/orphans into p{}
            if do_inject_widows:
                try:
                    if not (re.search(r'\bwidows\s*:', css, re.IGNORECASE) or
                            re.search(r'\borphans\s*:', css, re.IGNORECASE)):
                        def _wo(m):
                            return re.sub(r'(\})\s*$', r'widows:2;orphans:2;\1',
                                          m.group(0))
                        css, n = re.subn(r'(?<![.#\w])p\s*\{[^}]*\}', _wo, css,
                                         flags=re.IGNORECASE | re.DOTALL)
                        if n == 0:
                            css += '\np{widows:2;orphans:2;}\n'
                except Exception as e:
                    print(f"\n  Warning: widows/orphans CSS injection failed for "
                          f"{css_path}: {e}", file=sys.stderr)

            # 5. Inject page-break hints
            if do_inject_page_break:
                try:
                    if not re.search(
                            r'\bpage-break-|\bbreak-inside\b|\bbreak-after\b|\bbreak-before\b',
                            css, re.IGNORECASE):
                        css += _PBK
                except Exception as e:
                    print(f"\n  Warning: page-break CSS injection failed for "
                          f"{css_path}: {e}", file=sys.stderr)

            # 6. Add font-display:swap to @font-face blocks
            if do_font_display_swap:
                try:
                    def _fds(m):
                        b = m.group(0)
                        if re.search(r'font-display\s*:', b, re.IGNORECASE): return b
                        return re.sub(r'\}(\s*)$', r'font-display:swap;}\1', b)
                    css = re.sub(r'@font-face\s*\{[^}]*\}', _fds, css,
                                 flags=re.IGNORECASE | re.DOTALL)
                except Exception as e:
                    print(f"\n  Warning: font-display swap failed for "
                          f"{css_path}: {e}", file=sys.stderr)

            # 7. Strip unsafe CSS properties
            if do_strip_unsafe:
                try:
                    css = _strip_unsafe_css_from_text(css, aggressive=kindle_mode)
                except Exception as e:
                    print(f"\n  Warning: unsafe CSS stripping failed for "
                          f"{css_path}: {e}", file=sys.stderr)

            # 8. Convert pt/px font-size units -> em (opt-in)
            if do_convert_font_units:
                try:
                    parts = re.split(r'(@media\b[^{]*\{)', css, flags=re.IGNORECASE)
                    new_parts, in_media, depth = [], False, 0
                    for part in parts:
                        if re.match(r'@media\b', part, re.IGNORECASE):
                            new_parts.append(part); in_media = True; depth = 1
                        elif in_media:
                            out = []
                            for ch in part:
                                out.append(ch)
                                if ch == '{': depth += 1
                                elif ch == '}':
                                    depth -= 1
                                    if depth == 0: in_media = False
                            new_parts.append(''.join(out))
                        else:
                            def _conv(m):
                                prop, val_s, unit = (m.group(1), m.group(2),
                                                     m.group(3).lower())
                                try: val = float(val_s)
                                except ValueError: return m.group(0)
                                if val == 0: return prop + '0'
                                em = val / (_PX_PER_EM if unit == 'px' else _PT_PER_EM)
                                pname = prop.split(':')[0].strip().lower()
                                if pname == 'line-height' and not (0.8 <= em <= 4.0):
                                    return m.group(0)
                                return (prop
                                        + f'{em:.3f}'.rstrip('0').rstrip('.')
                                        + 'em')
                            new_parts.append(_FONT_SIZE_DECL_RE.sub(_conv, part))
                    css = ''.join(new_parts)
                except Exception as e:
                    print(f"\n  Warning: font-size unit conversion failed for "
                          f"{css_path}: {e}", file=sys.stderr)

            # 9. Strip body/html background-color (opt-in)
            if do_strip_bg:
                try:
                    css = _strip_body_background(css)
                except Exception as e:
                    print(f"\n  Warning: body background stripping failed for "
                          f"{css_path}: {e}", file=sys.stderr)

            write_if_changed(css_path, original, css)

        except Exception as e:
            print(f"\n  Warning: CSS transform pass failed for {css_path}: {e}",
                  file=sys.stderr)


# ── Thin-wrapper shims ────────────────────────────────────────────────────────

def reduce_css_complexity(temp_dir):
    # Shim: complexity reduction handled inside apply_css_transforms.
    apply_css_transforms(temp_dir,
        do_strip_important=False, do_inject_hyphenation=False,
        do_inject_widows=False, do_inject_page_break=False,
        do_font_display_swap=False, do_strip_unsafe=False)

def strip_important(temp_dir):
    # Shim: !important removal handled inside apply_css_transforms.
    # Also strips inline style= !important from HTML files (separate loop).
    apply_css_transforms(temp_dir,
        do_reduce_complexity=False, do_inject_hyphenation=False,
        do_inject_widows=False, do_inject_page_break=False,
        do_font_display_swap=False, do_strip_unsafe=False)
    _ir = re.compile(r'\s*!important\b', re.IGNORECASE)
    for html_path in find_html(temp_dir):
        try:
            html = read_text(html_path)
            def _sia(m): return 'style="' + _ir.sub('', m.group(1)) + '"'
            write_if_changed(html_path, html,
                             re.sub(r'style="([^"]*)"', _sia, html,
                                    flags=re.IGNORECASE))
        except Exception as e:
            print(f"\n  Warning: inline !important stripping failed for "
                  f"{html_path}: {e}", file=sys.stderr)

def flatten_stylesheets(temp_dir):
    opf_paths, merged_away = find_opf(temp_dir), set()
    for html_path in find_html(temp_dir):
        html_dir = os.path.dirname(html_path)
        try:
            html      = read_text(html_path)
            link_tags = re.findall(r'<link\s[^>]*\brel="stylesheet"[^>]*/?>',
                                   html, flags=re.IGNORECASE)
            if len(link_tags) < 2: continue
            hrefs = [m.group(1)
                     for t in link_tags
                     if (m := re.search(r'\bhref="([^"]+)"', t, re.IGNORECASE))]
            if len(hrefs) < 2: continue
            abs_paths = [(h, os.path.normpath(os.path.join(html_dir, h)))
                         for h in hrefs
                         if os.path.exists(os.path.normpath(os.path.join(html_dir, h)))]
            if len(abs_paths) < 2: continue
            # Deduplicate: if all links point to the same file (e.g. duplicate
            # <link> tags in cover.xhtml), just clean the HTML and skip merging.
            seen_paths = {}
            unique_abs = []
            for h, ap in abs_paths:
                if ap not in seen_paths:
                    seen_paths[ap] = h
                    unique_abs.append((h, ap))
            if len(unique_abs) == 1:
                # All links were duplicates of one file — deduplicate tags in HTML only
                # Recompute href relative to THIS html file's directory (Bug #2 fix)
                canon_href = os.path.relpath(unique_abs[0][1], html_dir).replace(os.sep, '/')
                for tag in link_tags: html = html.replace(tag, '')
                html = re.sub(r'(</head>)',
                              f'<link rel="stylesheet" type="text/css" href="{canon_href}"/>\\1',
                              html, flags=re.IGNORECASE)
                write_text(html_path, html)
                continue
            abs_paths = unique_abs
            # canon_href must be relative to this html file's directory (Bug #2 fix)
            canon_href = os.path.relpath(abs_paths[0][1], html_dir).replace(os.sep, '/')
            canon_path = abs_paths[0][1]
            parts = []
            for href, ap in abs_paths:
                try:
                    parts.append(f'/* merged: {os.path.basename(ap)} */\n')
                    parts.append(read_text(ap))
                    parts.append('\n')
                except Exception: pass
            write_text(canon_path, '\n'.join(parts))
            for _, ap in abs_paths[1:]:
                try:
                    os.remove(ap); merged_away.add(ap)
                except Exception: pass
            for tag in link_tags: html = html.replace(tag, '')
            html = re.sub(r'(</head>)',
                          f'<link rel="stylesheet" type="text/css" href="{canon_href}"/>\\1',
                          html, flags=re.IGNORECASE)
            write_text(html_path, html)
        except Exception as e:
            _warn("CSS flattening failed", html_path, e)
    for opf_path in opf_paths:
        try:
            content, opf_dir, changed = read_text(opf_path), os.path.dirname(opf_path), False
            def rm_merged(match):
                nonlocal changed
                tag    = match.group(0)
                href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                if href_m and (
                        os.path.normpath(os.path.join(opf_dir, href_m.group(1)))
                        in merged_away):
                    changed = True; return ''
                return tag
            content = re.sub(r'<item\s[^>]*/>', rm_merged, content, flags=re.IGNORECASE)
            if changed: write_text(opf_path, content)
        except Exception: pass

def fix_render_blocking(temp_dir):
    for css_path in find_css(temp_dir):
        css_dir = os.path.dirname(css_path)
        try:
            css, changed = read_text(css_path), False
            def resolve_import(match):
                nonlocal changed
                raw = match.group(1).strip().strip('\'"')
                if raw.startswith('http'): return match.group(0)
                ip = os.path.normpath(os.path.join(css_dir, raw))
                if not os.path.exists(ip): return ''
                try:
                    txt = read_text(ip); os.remove(ip); changed = True
                    return f'/* inlined: {os.path.basename(raw)} */\n{txt}\n'
                except Exception: return match.group(0)
            css = re.sub(r'@import\s+(?:url\()?([^;)\n]+)\)?;', resolve_import, css)
            if changed: write_text(css_path, css)
        except Exception as e:
            _warn("@import resolution failed", css_path, e)
    for html_path in find_html(temp_dir):
        try:
            html   = read_text(html_path)
            head_m = re.search(r'</head>', html, re.IGNORECASE)
            if not head_m: continue
            styles = re.findall(
                r'(<style\b[^>]*>.*?</style>)', html, flags=re.IGNORECASE | re.DOTALL)
            in_body = [s for s in styles if html.find(s) > head_m.end()]
            if not in_body: continue
            for s in in_body: html = html.replace(s, '', 1)
            html = re.sub(r'(</head>)', '\n'.join(in_body) + r'\n\1', html, flags=re.IGNORECASE)
            write_text(html_path, html)
        except Exception as e:
            _warn("render-blocking fix failed", html_path, e)

def collect_html_text(temp_dir):
    # Use cached HTML content from the index — avoids re-reading all HTML files
    # for every call (remove_unused_css, remove_unused_images, etc. all call this).
    cache = get_html_text_cache(temp_dir)
    if cache:
        return '\n'.join(cache.values())
    # Fallback: read from disk if cache is empty (shouldn't happen in normal pipeline)
    chunks = []
    for html_path in find_html(temp_dir):
        try: chunks.append(read_text(html_path))
        except Exception: pass
    return '\n'.join(chunks)


def _build_html_attr_tokens(html_text):
    # Build the set of all tokens that can appear in CSS selectors for this HTML
    # corpus.  Used by remove_unused_css_rules to avoid pruning rules that DO
    # match elements, without false-positives from body text.
    #
    # Collects:
    #   • class="…"          → each space-separated class name
    #   • id="…"             → each id value
    #   • data-*="…"         → the full attribute name (e.g. "data-type") AND the
    #                          suffix (e.g. "type") AND the value (e.g. "chapter"),
    #                          so [data-type], [data-type="chapter"], and plain token
    #                          matches all survive pruning
    #   • epub:type="…"      → each space-separated semantic type value, so that
    #                          CSS like [epub\:type~="chapter"] is never pruned
    #   • tag names          → element names from opening tags
    tokens = set()

    # class= and id= values
    for val in re.findall(r'(?:class|id)="([^"]*)"', html_text, re.IGNORECASE):
        tokens.update(val.split())

    # data-* attribute names and their values
    for attr, val in re.findall(r'\b(data-[a-zA-Z0-9_-]+)="([^"]*)"', html_text, re.IGNORECASE):
        tokens.add(attr)                    # e.g. "data-type"
        suffix = attr[5:]                   # strip "data-" prefix → "type"
        if suffix: tokens.add(suffix)
        if val.strip(): tokens.update(val.split())

    # epub:type values (EPUB3 semantic sectioning — often targeted by CSS)
    for val in re.findall(r'epub:type="([^"]*)"', html_text, re.IGNORECASE):
        tokens.update(val.split())

    # Tag names from opening tags
    tokens.update(re.findall(r'<([a-zA-Z][a-zA-Z0-9]*)\b', html_text))

    return tokens

def remove_unused_css_rules(css_content, html_attr_tokens):
    # Remove CSS rules whose selectors reference no class/id/tag found in the HTML.
    # html_attr_tokens is a set of class names, id values, and element tag names
    # extracted from HTML attributes — not raw body text — to avoid false-positives
    # where a CSS class name happens to appear as a body-text substring. (Bug #7 fix)
    result, i, n = [], 0, len(css_content)
    while i < n:
        while i < n and css_content[i].isspace(): i += 1
        if i >= n: break
        bp = css_content.find('{', i)
        if bp == -1: break
        prelude  = css_content[i:bp].strip()
        depth, j = 0, bp
        while j < n:
            if css_content[j] == '{':   depth += 1
            elif css_content[j] == '}':
                depth -= 1
                if depth == 0: break
            j += 1
        block = css_content[i:j + 1]
        i     = j + 1
        if prelude.startswith('@'):
            # For @media, @supports, @layer, and @container, recurse into the
            # block body and apply the same unused-rule pruning to nested rules.
            # These are the only at-rules that contain selector-based sub-rules
            # that can be pruned.
            #
            # @layer can be used with or without a block:
            #   @layer utilities;              ← statement form, no block — keep as-is
            #   @layer utilities { .foo { } }  ← block form — recurse
            #
            # @keyframes, @font-face, @charset, @namespace, @import: keep as-is
            # (they don't contain selector-based rules we can prune).
            at_keyword = prelude.split('(')[0].split()[0].lower()  # e.g. '@media'
            if at_keyword in ('@media', '@supports', '@layer', '@container'):
                open_b = block.find('{')
                close_b = len(block) - 1  # depth tracking already found the matching }
                inner = block[open_b + 1:close_b]
                pruned_inner = remove_unused_css_rules(inner, html_attr_tokens)
                # Only keep the @-rule wrapper if it still has content after pruning
                if pruned_inner.strip(): result.append(prelude + '{' + pruned_inner + '}')
            else: result.append(block)
            continue
        keep = []
        for sel in [s.strip() for s in prelude.split(',')]:
            if not sel: continue
            tokens    = re.findall(r'[a-zA-Z0-9_-]+', sel)
            meaningful = [t for t in tokens if len(t) > 1 and t.lower() not in CSS_SKIP_TOKENS]
            if not meaningful or any(t in html_attr_tokens for t in meaningful): keep.append(sel)
        if keep:
            open_brace = block.find('{')
            if open_brace == -1: continue  # malformed rule — skip safely (Bug #3 fix)
            result.append(', '.join(keep) + ' ' + block[open_brace:])
    return '\n'.join(result)

def remove_unused_css(temp_dir):
    html_text = collect_html_text(temp_dir)
    if not html_text: return
    html_attr_tokens = _build_html_attr_tokens(html_text)
    for css_path in find_css(temp_dir):
        try:
            write_text(css_path, remove_unused_css_rules(read_text(css_path), html_attr_tokens))
        except Exception as e:
            _warn("could not clean CSS", css_path, e)




# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — HTML FIXES
# ═══════════════════════════════════════════════════════════════════════════════


def apply_html_transforms(temp_dir,
                          do_charset=True,
                          do_lang=True,
                          do_typography=True,
                          do_alt_text=True,
                          do_lazy_loading=True,
                          do_normalize=True,
                          do_sanitize_address=True,
                          do_remove_empty_spans=True,
                          do_xml_space=True,
                          do_strip_important_inline=True,
                          do_strip_deprecated_attrs=False,
                          do_scrub_print=False):
    # Single-pass HTML transform: reads each HTML file once, applies all active
    # sub-transforms in sequence, writes back only if content changed.
    # Each sub-transform has its own try/except for per-transform fault isolation.
    #
    # Ordering constraints respected:
    #   • normalize (step 6) must run before remove_empty_spans (step 8)
    #   • repair_xhtml_wellformedness always runs separately, AFTER this function
    #
    # Sub-transform order:
    #   1.  charset              — enforce UTF-8 charset meta
    #   2.  lang                 — fix/add xml:lang + lang attributes
    #   3.  typography           — smart quotes, dashes, ellipsis (skips nav)
    #   4.  alt_text             — add missing alt="" to <img>
    #   5.  lazy_loading         — add loading="eager|lazy" to <img>
    #   6.  normalize            — strip redundant styles/classes, empty tags
    #   7.  sanitize_address     — <address> -> <div class="address">
    #   8.  remove_empty_spans   — strip attribute-only empty <span>/<div>
    #   9.  xml_space            — add xml:space="preserve" to <pre>
    #  10.  strip_important_inline — remove !important from style= attrs
    #  11.  strip_deprecated_attrs — align=/bgcolor=/border= removal (opt-in)
    #  12.  scrub_print          — printer's keys / boilerplate (opt-in, skips nav)

    # ── Pre-loop: collect state shared across all files ──────────────────────
    _lang          = _get_book_language(temp_dir) if do_lang else None
    _nav_paths     = set(find_nav(temp_dir))
    _css_rules     = collect_stylesheet_rules(temp_dir) if do_normalize else set()
    _cover_paths   = set()
    _IMP_INLINE    = re.compile(r'\s*!important\b', re.IGNORECASE)
    _SAFE_ATTR_LOC = re.compile(
        r'^(?:\s*(?:class|style|id|lang|xml:lang)\s*=\s*"[^"]*"\s*)*$',
        re.IGNORECASE)

    if do_lazy_loading:
        for _opf in find_opf(temp_dir):
            try:
                _od  = os.path.dirname(_opf)
                _otx = read_text(_opf)
                for _m in re.finditer(
                        r'<item\s[^>]*\bhref="([^"]+)"[^>]*/>', _otx, re.IGNORECASE):
                    _hr, _tg = _m.group(1), _m.group(0)
                    if ('cover' in _hr.lower() or 'cover' in _tg.lower()) and \
                            re.search(r'application/xhtml|text/html', _tg, re.IGNORECASE):
                        _cover_paths.add(os.path.normpath(os.path.join(_od, _hr)))
            except Exception: pass

    # ── Per-file loop ────────────────────────────────────────────────────────
    for html_path in find_html(temp_dir):
        try:
            original = read_text(html_path)
            html     = original
            is_nav   = html_path in _nav_paths

            # 1. Charset
            if do_charset:
                try:
                    html = re.sub(r'<meta\s[^>]*\bcharset=[^>]*/?>',
                                  '', html, flags=re.IGNORECASE)
                    html = re.sub(
                        r'<meta\s[^>]*http-equiv=["\']content-type["\'][^>]*/?>',
                        '', html, flags=re.IGNORECASE)
                    html = re.sub(r'(<head\b[^>]*>)', r'\1<meta charset="utf-8"/>',
                                  html, count=1, flags=re.IGNORECASE)
                    # Strip <meta name="viewport"> from non-nav spine docs
                    if not is_nav:
                        html = re.sub(r'<meta\s[^>]*\bname=["\']viewport["\'][^>]*/?>',
                                      '', html, flags=re.IGNORECASE)
                except Exception as e:
                    print(f"\n  Warning: charset enforcement failed for "
                          f"{html_path}: {e}", file=sys.stderr)

            # 2. Lang attributes
            if do_lang and _lang:
                try:
                    _hm = re.search(r'<html\b([^>]*)>', html, re.IGNORECASE)
                    if _hm:
                        _at = _hm.group(1)
                        _at = re.sub(r'\s*\bxml:lang="[^"]*"', '', _at)
                        _at = re.sub(r'\s*\blang="[^"]*"',     '', _at)
                        html = html.replace(
                            _hm.group(0),
                            f'<html{_at} lang="{_lang}" xml:lang="{_lang}">', 1)
                except Exception as e:
                    print(f"\n  Warning: lang fix failed for {html_path}: {e}",
                          file=sys.stderr)

            # 3. Typography (skip nav docs)
            if do_typography and not is_nav:
                try:
                    html   = _fix_windows1252(html)
                    _parts = _TAG_SPLIT_RE.split(html)
                    _inp   = False
                    for _i in range(len(_parts)):
                        if _i % 2 == 1:
                            _tl = _parts[_i].lower().lstrip('<').split()[0].rstrip('/>')
                            if   _tl in ('pre','code','kbd','samp'):   _inp = True
                            elif _tl in ('/pre','/code','/kbd','/samp'): _inp = False
                        elif not _inp:
                            _parts[_i] = _normalize_dashes_and_ellipsis(_parts[_i])
                            _parts[_i] = _normalize_quotes(_parts[_i])
                            _parts[_i] = _INVISIBLE_CHARS_RE.sub('', _parts[_i])
                            _parts[_i] = _NBSP_INDENT_RE.sub('', _parts[_i])
                    html = ''.join(_parts)
                except Exception as e:
                    print(f"\n  Warning: typography normalisation failed for "
                          f"{html_path}: {e}", file=sys.stderr)

            # 4. Alt text
            if do_alt_text:
                try:
                    def _ia(m):
                        t = m.group(0)
                        if re.search(r'\balt\s*=', t, re.IGNORECASE): return t
                        return re.sub(r'(<img\b)', r'\1 alt=""', t,
                                      flags=re.IGNORECASE)
                    html = re.sub(r'<img\b[^>]*/?>', _ia, html, flags=re.IGNORECASE)
                except Exception as e:
                    print(f"\n  Warning: alt text injection failed for "
                          f"{html_path}: {e}", file=sys.stderr)

            # 5. Lazy loading
            if do_lazy_loading:
                try:
                    _ic = (html_path in _cover_paths or
                           'cover' in os.path.basename(html_path).lower())
                    def _il(m):
                        t = m.group(0)
                        if re.search(r'\bloading\s*=', t, re.IGNORECASE): return t
                        return re.sub(r'(<img\b)',
                                      r'\1 loading="' + ('eager' if _ic else 'lazy') + '"',
                                      t, flags=re.IGNORECASE)
                    html = re.sub(r'<img\b[^>]*/?>', _il, html, flags=re.IGNORECASE)
                except Exception as e:
                    print(f"\n  Warning: lazy loading failed for "
                          f"{html_path}: {e}", file=sys.stderr)

            # 6. Normalize HTML (must run before remove_empty_spans)
            if do_normalize:
                try:
                    html = re.sub(r'\s*\bstyle="\s*"', '', html)
                    html = re.sub(r'\s*\bclass="\s*"', '', html)
                    if _css_rules:
                        def _ci(m):
                            return strip_redundant_inline_styles(m.group(0),
                                                                 _css_rules)
                        html = re.sub(r'<[^>]+\bstyle="[^"]*"[^>]*>', _ci, html)
                    for _tg in EMPTY_SAFE_TAGS:
                        def _re_tag(m):
                            _f = m.group(0)
                            if re.search(
                                    r'\b(id|src|href|data-|name|role|aria-)\s*=',
                                    _f, re.IGNORECASE): return _f
                            return ''
                        html = re.sub(rf'<{_tg}(\s[^>]*)?\s*>\s*</{_tg}>',
                                      _re_tag, html, flags=re.IGNORECASE)
                except Exception as e:
                    print(f"\n  Warning: HTML normalisation failed for "
                          f"{html_path}: {e}", file=sys.stderr)

            # 7. Sanitize <address> tags
            if do_sanitize_address:
                try:
                    if '<address' in html.lower():
                        def _ra(m):
                            _at = m.group(1)
                            _cm = re.search(r'\bclass="([^"]*)"', _at, re.IGNORECASE)
                            if _cm:
                                _ex  = _cm.group(1).strip()
                                _nc  = ('address ' + _ex).strip()
                                _at  = _at.replace(_cm.group(0), f'class="{_nc}"')
                            else: _at = ' class="address"' + _at
                            return f'<div{_at}>'
                        html = _ADDRESS_OPEN_RE.sub(_ra, html)
                        html = _ADDRESS_CLOSE_RE.sub('</div>', html)
                except Exception as e:
                    print(f"\n  Warning: address tag sanitization failed for "
                          f"{html_path}: {e}", file=sys.stderr)

            # 8. Remove attribute-only empty <span>/<div> shells (after step 6)
            if do_remove_empty_spans and not is_nav:
                try:
                    def _rs(m):
                        if _SAFE_ATTR_LOC.match(m.group(2).strip()):
                            return ''
                        return m.group(0)
                    for _ in range(3):
                        _nh = _EMPTY_SPAN_RE.sub(_rs, html)
                        if _nh == html: break
                        html = _nh
                except Exception as e:
                    print(f"\n  Warning: empty span removal failed for "
                          f"{html_path}: {e}", file=sys.stderr)

            # 9. xml:space="preserve" on <pre>
            if do_xml_space:
                try:
                    def _xs(m):
                        t = m.group(0)
                        if 'xml:space' in t.lower(): return t
                        return re.sub(r'\s*/?>$', ' xml:space="preserve">',
                                      t.rstrip())
                    html = re.sub(r'<pre\b[^>]*>', _xs, html, flags=re.IGNORECASE)
                except Exception as e:
                    print(f"\n  Warning: xml:space injection failed for "
                          f"{html_path}: {e}", file=sys.stderr)

            # 10. Strip !important from inline style= attributes
            if do_strip_important_inline:
                try:
                    def _si(m):
                        return 'style="' + _IMP_INLINE.sub('', m.group(1)) + '"'
                    html = re.sub(r'style="([^"]*)"', _si, html,
                                  flags=re.IGNORECASE)
                except Exception as e:
                    print(f"\n  Warning: inline !important stripping failed for "
                          f"{html_path}: {e}", file=sys.stderr)

            # 11. Strip deprecated HTML attributes (opt-in)
            if do_strip_deprecated_attrs:
                try:
                    html = _strip_deprecated_html_attrs(html)
                except Exception as e:
                    print(f"\n  Warning: deprecated attr stripping failed for "
                          f"{html_path}: {e}", file=sys.stderr)

            # 12. Scrub print artifacts (opt-in, skips nav)
            if do_scrub_print and not is_nav:
                try:
                    html = _scrub_print_artifacts(html)
                except Exception as e:
                    print(f"\n  Warning: print artifact scrub failed for "
                          f"{html_path}: {e}", file=sys.stderr)

            # 13. Strip redundant HTML meta tags that belong in OPF, not spine docs.
            # <meta name="description">, <meta name="author">, <meta name="generator">
            # add bloat to every file and confuse some readers when they differ.
            if not is_nav:
                try:
                    html = re.sub(
                        r'<meta\s[^>]*\bname=["\'](?:description|author|generator|keywords)["\'][^>]*/?>',
                        '', html, flags=re.IGNORECASE)
                except Exception: pass

            write_if_changed(html_path, original, html)

        except Exception as e:
            print(f"\n  Warning: HTML transform pass failed for {html_path}: {e}",
                  file=sys.stderr)






def _fix_windows1252(text):
    # Single-pass replacement using pre-compiled regex — O(n) instead of O(n × m)
    return _WIN1252_RE.sub(lambda m: WIN1252_MAP[m.group(0)], text)

def _normalize_quotes(text):
    # Replace straight quotes with typographic quotes in TEXT NODES only.
    # The input `text` must already be a plain text segment (not containing
    # HTML tags). Called by normalize_typography which pre-splits on tags.
    #
    # Uses a stateful single-pass approach: track the previous non-space
    # character to decide open vs close, handling nested quotes correctly.
    if '"' not in text and "'" not in text:
        return text
    result, i, n = [], 0, len(text)
    while i < n:
        ch = text[i]
        preceding = text[i - 1] if i > 0 else ' '
        if ch == '"':
            # Open if preceded by whitespace/open-bracket/tag-boundary, else close
            result.append('\u201c' if preceding in ' \t\n\r([{>' else '\u201d')
        elif ch == "'":
            if preceding.isalpha():
                result.append('\u2019')  # apostrophe / close
            elif preceding in ' \t\n\r([{>':
                result.append('\u2018')  # open single
            else:
                result.append('\u2019')  # default close
        else:
            result.append(ch)
        i += 1
    return ''.join(result)


def _normalize_dashes_and_ellipsis(text):
    # Normalise typography in a plain text segment (no HTML tags expected).
    text = text.replace('---', '\u2014').replace('--', '\u2013').replace('...', '\u2026')
    return text





def collect_stylesheet_rules(temp_dir):
    rules = set()
    for css_path in find_css(temp_dir):
        try:
            css = re.sub(r'/\*.*?\*/', '', read_text(css_path), flags=re.DOTALL)
            for prop, val in re.findall(r'([\w-]+)\s*:\s*([^;}{]+)', css):
                rules.add((prop.strip().lower(), val.strip().lower()))
        except Exception: pass
    return rules


def strip_redundant_inline_styles(tag_html, stylesheet_rules):
    style_m = re.search(r'\bstyle="([^"]*)"', tag_html)
    if not style_m: return tag_html
    kept = []
    for decl in [d.strip() for d in style_m.group(1).split(';') if d.strip()]:
        if ':' not in decl:
            kept.append(decl)
            continue
        prop, _, val = decl.partition(':')
        prop_l = prop.strip().lower()
        val_l  = val.strip().lower()
        # Drop if it's a known no-op value for this property
        if val_l in _JUNK_INLINE_PROPS.get(prop_l, set()): continue
        # Drop if value is font-size: 1em (redundant relative size)
        if prop_l == 'font-size' and val_l in ('1em', '100%', 'medium',
                                                'initial', 'inherit'):
            continue
        # Drop if it duplicates a rule already in an external stylesheet
        if (prop_l, val_l) in stylesheet_rules: continue
        kept.append(decl)
    new_style = '; '.join(kept)
    if new_style: return tag_html.replace(style_m.group(0), f'style="{new_style}"')
    return re.sub(r'\s*\bstyle="[^"]*"', '', tag_html)


def scrub_print_artifacts(temp_dir):
    # Remove printer's keys and production boilerplate from HTML files.
    nav_paths = set(find_nav(temp_dir))
    for html_path in find_html(temp_dir):
        if html_path in nav_paths: continue
        try:
            html = read_text(html_path)
            new  = _scrub_print_artifacts(html)
            write_if_changed(html_path, html, new)
        except Exception as e:
            print(f"\n  Warning: print artifact scrub failed for {html_path}: {e}",
                  file=sys.stderr)


def inject_missing_html_title(temp_dir):
    # Ensure every spine HTML document has a non-empty <title> in <head>.
    # Missing or empty <title> breaks:
    #   • VoiceOver/TalkBack screen readers (used to announce document context)
    #   • Reader chapter menus on Kobo, Kindle, and Apple Books
    # Strategy: use the document's first heading text (h1 > h2 > h3) as the title.
    # If no heading found, use the filename stem as a last resort.
    nav_paths = set(find_nav(temp_dir))
    _heading_re = re.compile(r'<h[1-3]\b[^>]*>(.*?)</h[1-3]>', re.IGNORECASE | re.DOTALL)
    _tag_strip  = re.compile(r'<[^>]+>')
    _title_re   = re.compile(r'<title\b[^>]*>(.*?)</title>', re.IGNORECASE | re.DOTALL)

    for html_path in find_html(temp_dir):
        if html_path in nav_paths: continue
        try:
            html = read_text(html_path)
            title_m = _title_re.search(html)
            existing_title = _tag_strip.sub('', title_m.group(1)).strip() if title_m else ''
            if existing_title: continue  # already has a meaningful title

            # Find the best title text
            heading_m = _heading_re.search(html)
            if heading_m:
                title_text = _tag_strip.sub('', heading_m.group(1)).strip()
            else:
                title_text = os.path.splitext(os.path.basename(html_path))[0]
            if not title_text: continue

            if title_m and not existing_title:
                # Replace empty <title></title>
                html = _title_re.sub(f'<title>{title_text}</title>', html, count=1)
            else:
                # Inject <title> into <head>
                html = re.sub(r'(<head\b[^>]*>)',
                              rf'\1<title>{title_text}</title>',
                              html, count=1, flags=re.IGNORECASE)
            write_text(html_path, html)
        except Exception as e:
            _warn("HTML title injection failed", html_path, e)


def inject_epub_type_on_body(temp_dir):
    # Add epub:type="bodymatter chapter" to <body> elements in spine documents
    # that lack any epub:type annotation.
    # EPUB3 readers (Kobo, Apple Books, Kindle) use epub:type on body/section
    # for chapter navigation UI and reading position tracking.
    # Skip: nav documents, cover pages, front/back matter (detected heuristically).
    nav_paths = set(find_nav(temp_dir))
    _cover_hints = re.compile(r'cover|title.?page', re.IGNORECASE)

    for html_path in find_html(temp_dir):
        if html_path in nav_paths: continue
        basename = os.path.basename(html_path).lower()
        if _cover_hints.search(basename): continue
        try:
            html = read_text(html_path)
            # Only process EPUB3 documents (have epub: namespace or DOCTYPE)
            if 'epub:type' in html: continue  # already annotated
            if 'xmlns:epub' not in html: continue  # not EPUB3 XHTML

            def _add_epub_type(m):
                tag = m.group(0)
                if 'epub:type' in tag.lower(): return tag
                return re.sub(r'(\s*/?>)$',
                              ' epub:type="bodymatter chapter"\\1',
                              tag.rstrip(), count=1)
            new_html = re.sub(r'<body\b[^>]*>', _add_epub_type, html,
                              count=1, flags=re.IGNORECASE)
            write_if_changed(html_path, html, new_html)
        except Exception as e:
            _warn("epub:type injection failed", html_path, e)


def enforce_cover_spine_linear(temp_dir):
    # Ensure the cover spine item is linear="yes" (or has no linear attribute).
    # Many EPUBs ship with cover as linear="no" which hides it in linear reading
    # mode on Kobo, Apple Books, and most EPUB3 readers.
    for opf_path in find_opf(temp_dir):
        try:
            content = read_text(opf_path)
            # Find cover HTML href from manifest
            cover_id = None
            m = re.search(r'<meta\s[^>]*\bname="cover"\s[^>]*\bcontent="([^"]+)"',
                          content, re.IGNORECASE)
            if m: cover_id = m.group(1)
            if not cover_id:
                # Try properties="cover-image" item → find first spine HTML referencing it
                ci_m = re.search(
                    r'<item\s[^>]*\bproperties="[^"]*cover-image[^"]*"[^>]*\bid="([^"]+)"',
                    content, re.IGNORECASE)
                if ci_m: cover_id = ci_m.group(1)

            if not cover_id: continue

            # Find any itemref with this idref and linear="no" — fix it
            def fix_linear(m):
                tag = m.group(0)
                idref_m = re.search(r'\bidref="([^"]+)"', tag, re.IGNORECASE)
                if not idref_m: return tag
                if idref_m.group(1) != cover_id: return tag
                if 'linear="no"' in tag.lower():
                    return re.sub(r'\s*\blinear="no"', '', tag, flags=re.IGNORECASE)
                return tag

            new_content = re.sub(r'<itemref\s[^>]*/>', fix_linear, content, flags=re.IGNORECASE)
            write_if_changed(opf_path, content, new_content)
        except Exception as e:
            _warn("cover linear enforcement failed", opf_path, e)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10b — XHTML WELL-FORMEDNESS REPAIR  (Feature A)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Apple Books and many EPUB3 readers hard-fail on malformed XML.  We attempt
# a lightweight regex-based repair that fixes the most common issues without
# requiring html5lib or lxml:
#   1. Undefined XML entities (&nbsp; &mdash; etc.) → numeric references
#   2. Void elements missing self-close slash (<br> → <br/>)
#   3. Missing xmlns declaration on <html> element
#   4. XML declaration at top (if absent)
# If the file still fails to parse after repair, we log a warning and leave
# the original in place rather than corrupting it further.


def _repair_xhtml(html):
    # Apply lightweight XHTML well-formedness fixes.
    # Returns the (possibly repaired) string.
    # 1. Replace known named HTML entities with numeric references
    def replace_entity(m):
        ref = '&' + m.group(1) + ';'
        if ref in _ENTITY_MAP: return _ENTITY_MAP[ref]
        # Unknown entity — leave as-is; ET.fromstring will catch it
        return ref
    html = _NAMED_ENTITY_RE.sub(replace_entity, html)

    # 2. Self-close void elements: <br> → <br/>, <br class="x"> → <br class="x"/>
    def self_close_void(m):
        tag_name = m.group(1)
        attrs    = m.group(2) or ''
        return f'<{tag_name}{attrs}/>'
    html = _VOID_TAG_RE.sub(self_close_void, html)

    # 3. Ensure xmlns on <html> element
    if '<html' in html and 'xmlns=' not in html:
        html = re.sub(
            r'(<html\b)',
            r'\1 xmlns="http://www.w3.org/1999/xhtml"',
            html, count=1, flags=re.IGNORECASE)

    # 4. Ensure XML declaration at top
    if not html.lstrip().startswith('<?xml'):
        html = '<?xml version="1.0" encoding="utf-8"?>\n' + html.lstrip()

    return html


def repair_xhtml_wellformedness(temp_dir):
    # Three-pass repair: ElementTree strict parse → _repair_xhtml() regex fixes
    # → lxml permissive parse. Files that fail all three are left intact.
    for html_path in find_html(temp_dir):
        try:
            raw = read_raw(html_path)
            try:
                ET.fromstring(raw)
                continue  # already valid XML — nothing to do
            except ET.ParseError: pass  # needs repair

            html     = raw.decode('utf-8', errors='replace')
            repaired = _repair_xhtml(html)
            try:
                ET.fromstring(repaired.encode('utf-8'))
                write_text(html_path, repaired)
                continue
            except ET.ParseError: pass  # lightweight repair wasn't enough

            # ── Level 3: lxml error-recovering parse → re-serialise ───────────
            if HAS_LXML:
                try:
                    doc = _lxml_html.fromstring(repaired)   # permissive parse
                    # Convert to XHTML: serialise as XML, wrap with XHTML boilerplate
                    body_html = _lxml_etree.tostring(
                        doc, encoding='unicode', method='xml')
                    # lxml.html.fromstring returns the body element; wrap properly
                    if doc.tag in ('html', '{http://www.w3.org/1999/xhtml}html'):
                        xhtml = _lxml_etree.tostring(doc, encoding='unicode', method='xml')
                    else:
                        # lxml returned a fragment — reconstruct full document
                        head_m = re.search(r'<head\b.*?</head>', repaired,
                                           re.IGNORECASE | re.DOTALL)
                        head   = head_m.group(0) if head_m else '<head/>'
                        xhtml  = (
                            '<?xml version="1.0" encoding="utf-8"?>\n'
                            '<html xmlns="http://www.w3.org/1999/xhtml">\n'
                            + head + '\n<body>\n' + body_html + '\n</body>\n</html>')
                    # Verify the result is well-formed XML
                    ET.fromstring(xhtml.encode('utf-8'))
                    write_text(html_path, xhtml)
                    continue
                except Exception: pass  # lxml repair also failed — fall through

            _warn(f"XHTML repair exhausted all strategies", html_path)
            # Leave original intact — don't make things worse
        except Exception as e:
            _warn("XHTML well-formedness check failed", html_path, e)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10c — JAVASCRIPT / SCRIPT STRIPPING  (Feature F)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Most EPUB readers ignore JavaScript and some choke on malformed scripts.
# Removing <script> tags, inline on* handlers, and JS manifest entries is
# always safe and reduces attack surface.


def strip_javascript(temp_dir):
    # Remove JavaScript from all HTML/XHTML files:
    # • <script>…</script> blocks (inline JS)
    # • Self-closing <script … /> tags
    # • Inline on* event handler attributes (onclick=, onload=, etc.)
    #
    # Also removes JS manifest entries from OPF:
    # • <item> elements whose media-type is application/javascript or text/javascript
    for html_path in find_html(temp_dir):
        try:
            html = read_text(html_path)
            new  = _SCRIPT_TAG_RE.sub('', html)
            new  = _SCRIPT_SELF_CLOSE.sub('', new)
            new  = _INLINE_HANDLER_RE.sub('', new)
            write_if_changed(html_path, html, new)
        except Exception as e:
            _warn("JS stripping failed", html_path, e)

    # Strip JS files from OPF manifest
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            content, changed = read_text(opf_path), False
            def remove_js_item(m):
                nonlocal changed
                tag = m.group(0)
                mt_m = re.search(r'\bmedia-type="([^"]+)"', tag, re.IGNORECASE)
                if mt_m and 'javascript' in mt_m.group(1).lower():
                    # Also delete the JS file from disk
                    href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                    if href_m:
                        js_path = os.path.normpath(os.path.join(opf_dir, href_m.group(1)))
                        try:
                            if os.path.exists(js_path): os.remove(js_path)
                        except Exception: pass
                    changed = True
                    return ''
                return tag
            content = re.sub(r'<item\s[^>]*/\s*>', remove_js_item, content, flags=re.IGNORECASE)
            if changed: write_text(opf_path, content)
        except Exception as e:
            _warn("JS manifest stripping failed", opf_path, e)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10d — CSS UNSAFE-PROPERTY FILTER  (Feature E / Tier 2)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Some CSS properties cause layout collapse on Kindle and other e-readers.
# Enabled by --strip-unsafe-css or --profile kindle.
#
# Safe-strip (always removed when enabled):
#   position: fixed  — causes overlay rendering issues on paginated readers
#   vh / vw units    — viewport-relative units are meaningless in EPUB
#
# Aggressive-strip (Kindle profile only):
#   position: absolute  — breaks flow layout on reflowable books
#   float               — unreliable on many reading systems
#
# We operate declaration-by-declaration inside CSS blocks, not on selectors,
# so surrounding rules are preserved.


def _strip_unsafe_css_from_text(css, aggressive=False):
    # aggressive=True also strips position:absolute and float
    # Remove position:fixed
    css = _FIXED_VAL_RE.sub('/* position:fixed removed */', css)
    # Remove declarations containing vh/vw units
    css = _UNSAFE_UNITS_RE.sub(lambda m: '/* vh/vw unit removed */', css)
    if aggressive:
        css = _ABSOLUTE_VAL_RE.sub('/* position:absolute removed */', css)
        css = _FLOAT_PROP_RE.sub('/* float removed */', css)
    return css


def strip_unsafe_css(temp_dir, aggressive=False):
    # Shim: unsafe CSS stripping handled inside apply_css_transforms.
    apply_css_transforms(temp_dir,
        do_reduce_complexity=False, do_strip_important=False,
        do_inject_hyphenation=False, do_inject_widows=False,
        do_inject_page_break=False, do_font_display_swap=False,
        kindle_mode=aggressive)

def strip_deprecated_html_attrs(temp_dir):
    # Remove align=, bgcolor=, border=, valign= etc. from all HTML files.
    for html_path in find_html(temp_dir):
        try:
            html = read_text(html_path)
            new  = _strip_deprecated_html_attrs(html)
            write_if_changed(html_path, html, new)
        except Exception as e:
            print(f"\n  Warning: deprecated attr stripping failed for {html_path}: {e}",
                  file=sys.stderr)


def strip_body_backgrounds(temp_dir):
    # Shim: background stripping handled inside apply_css_transforms.
    apply_css_transforms(temp_dir,
        do_reduce_complexity=False, do_strip_important=False,
        do_inject_hyphenation=False, do_inject_widows=False,
        do_inject_page_break=False, do_font_display_swap=False,
        do_strip_unsafe=False, do_strip_bg=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10e — HTML FEATURES (address, justify, SVG, guide)
# ═══════════════════════════════════════════════════════════════════════════════
#
#  1. sanitize_address_tags       — <address> → <div class="address">
#  2. inject_justify_css          — add text-align:justify to p rules (opt-in)
#  3. optimize_svg_files          — strip Inkscape/Adobe/editor metadata from SVG
#  4. repair_guide_references     — validate/fix OPF <guide> <reference> hrefs
#  5. repair_degenerate_toc       — rebuild TOC when < TOC_MIN_ENTRIES entries
#  6. warn_large_html_files       — print warnings for HTML files > HTML_SPLIT_WARN_KB
#  7. optimize_zip_small_files    — use ZIP_STORED for files < ZIP_STORED_MAX_BYTES




# ════════════════════════════════════════════════════════════════════════════════
# 1. <address> tag sanitization
# ════════════════════════════════════════════════════════════════════════════════


def sanitize_address_tags(temp_dir):
    apply_html_transforms(temp_dir,
        do_charset=False,
        do_lang=False,
        do_typography=False,
        do_alt_text=False,
        do_lazy_loading=False,
        do_normalize=False,
        do_remove_empty_spans=False,
        do_xml_space=False,
        do_strip_important_inline=False)

def inject_justify_css(temp_dir):
    # Ensure at least one CSS file contains text-align:justify on paragraph rules.
    #
    # Only runs if NO existing CSS file already contains text-align:justify (or
    # text-align:justify-all), which respects intentionally left-aligned books
    # such as poetry collections.
    #
    # Injects into an existing `p { … }` rule if one is present, otherwise
    # appends a standalone `p { text-align:justify; }` rule.
    #
    # This is opt-in via --inject-justify; not part of the default pipeline.
    css_paths = find_css(temp_dir)
    if not css_paths: return

    # Check if any CSS already has justify
    for css_path in css_paths:
        try:
            if re.search(r'text-align\s*:\s*justify', read_text(css_path), re.IGNORECASE):
                return  # already justified — nothing to do
        except Exception: pass

    # Inject into the first CSS file that has a p{} rule; else the first CSS file
    target_path = None
    for css_path in css_paths:
        try:
            if re.search(r'(?<![.#\w])p\s*\{', read_text(css_path), re.IGNORECASE):
                target_path = css_path
                break
        except Exception: pass
    if target_path is None: target_path = css_paths[0]

    try:
        css = read_text(target_path)
        def inject_into_p(m):
            block = m.group(0)
            if re.search(r'text-align\s*:', block, re.IGNORECASE):
                return block  # already has a text-align — don't override
            return re.sub(r'\}(\s*)$', r'text-align:justify;}\1', block)
        new_css, n = re.subn(r'(?<![.#\w])p\s*\{[^}]*\}', inject_into_p, css,
                             flags=re.IGNORECASE | re.DOTALL)
        if n == 0: new_css = css + '\np{text-align:justify;}\n'
        write_text(target_path, new_css)
    except Exception as e: print(f"\n  Warning: justify CSS injection failed: {e}", file=sys.stderr)


# ════════════════════════════════════════════════════════════════════════════════
# 3. SVG optimization
# ════════════════════════════════════════════════════════════════════════════════


def _optimize_svg_content(svg):
    # Apply in-place SVG optimizations that are safe and dependency-free.
    # Remove XML comments
    svg = _SVG_COMMENT_RE.sub('', svg)
    # Remove editor namespace declarations from <svg> opening tag
    svg = _SVG_EDITOR_NS_DECL_RE.sub('', svg)
    # Remove editor-namespace elements (sodipodi:namedview, inkscape:*, rdf:RDF, etc.)
    svg = _SVG_EDITOR_ELEM_RE.sub('', svg)
    # Remove metadata blocks
    svg = re.sub(r'<metadata\b[^>]*>.*?</metadata>', '', svg,
                 flags=re.IGNORECASE | re.DOTALL)
    # Collapse runs of whitespace (not inside attribute values or CDATA)
    # Safe pass: collapse newline sequences between elements to single newline
    svg = re.sub(r'>(\s{2,})<', '>\n<', svg)
    # Strip xml:space="preserve" on non-text elements (causes layout drift on some viewers)
    svg = re.sub(
        r'(<(?!text\b)[^>]+)\s+xml:space="preserve"', r'\1', svg, flags=re.IGNORECASE)
    # Ensure required namespaces present on root <svg> element
    if '<svg' in svg and 'xmlns="' not in svg:
        svg = re.sub(r'(<svg\b)', r'\1 xmlns="http://www.w3.org/2000/svg"', svg,
                     count=1, flags=re.IGNORECASE)
    return svg


def optimize_svg_files(temp_dir):
    # Optimize SVG files in the EPUB by removing editor-only metadata:
    # • Inkscape (sodipodi:namedview, inkscape:perspective, etc.)
    # • Adobe Illustrator (ai:*, xap:*, xmpMM:*)
    # • RDF/Dublin Core metadata blocks
    # • XML comments
    # • xml:space="preserve" on non-text elements
    #
    # No external dependencies.  Falls back gracefully on any parse error.
    # Typical savings: 20–60% on Inkscape/Illustrator-generated SVG covers.
    # SVG files in the EPUB (standalone .svg files)
    for root, _, files in os.walk(temp_dir):
        for file in files:
            if not file.lower().endswith('.svg'): continue
            path = os.path.join(root, file)
            try:
                original = read_text(path)
                optimized = _optimize_svg_content(original)
                if len(optimized) < len(original): write_text(path, optimized)
            except Exception as e:
                print(f"\n  Warning: SVG optimization failed for {path}: {e}",
                      file=sys.stderr)

    # Also optimize inline <svg>…</svg> blocks within HTML/XHTML files
    _SVG_INLINE_RE = re.compile(r'<svg\b[^>]*>.*?</svg>', re.IGNORECASE | re.DOTALL)
    for html_path in find_html(temp_dir):
        try:
            html = read_text(html_path)
            if '<svg' not in html.lower(): continue
            def opt_inline(m): return _optimize_svg_content(m.group(0))
            new = _SVG_INLINE_RE.sub(opt_inline, html)
            write_if_changed(html_path, html, new)
        except Exception as e:
            print(f"\n  Warning: inline SVG optimization failed for {html_path}: {e}",
                  file=sys.stderr)


# ════════════════════════════════════════════════════════════════════════════════
# 4. OPF <guide> repair
# ════════════════════════════════════════════════════════════════════════════════


def repair_guide_references(temp_dir):
    # Validate and repair OPF <guide> <reference> elements:
    # • Remove references whose href resolves to a file not on disk.
    # • Normalise type= to lowercase; warn on unknown types (keep them — custom
    # types are allowed by some reading systems).
    # • If <guide> is present but empty after cleanup, remove the whole block.
    # • If guide is absent and cover + text can be inferred, inject a minimal one.
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            content, changed = read_text(opf_path), False

            # Skip if no <guide> block
            if not re.search(r'<guide\b', content, re.IGNORECASE):
                _maybe_inject_guide(content, opf_path, opf_dir)
                continue

            def validate_ref(m):
                nonlocal changed
                tag    = m.group(0)
                href_m = re.search(r'\bhref="([^"#]+)', tag, re.IGNORECASE)
                if not href_m: return tag
                href     = href_m.group(1)
                abs_path = os.path.normpath(os.path.join(opf_dir, href))
                if not os.path.exists(abs_path):
                    changed = True
                    return ''  # remove dangling reference
                # Lowercase type attribute
                type_m = re.search(r'\btype="([^"]+)"', tag, re.IGNORECASE)
                if type_m:
                    t_lower = type_m.group(1).lower()
                    if t_lower != type_m.group(1):
                        changed = True
                        tag = tag.replace(type_m.group(0), f'type="{t_lower}"')
                return tag

            content = re.sub(
                r'<reference\s[^>]*/\s*>', validate_ref, content, flags=re.IGNORECASE)

            # Remove empty <guide> blocks
            content = re.sub(
                r'<guide>\s*</guide>', '', content, flags=re.IGNORECASE)
            content = re.sub(
                r'<guide\s[^>]*>\s*</guide>', '', content, flags=re.IGNORECASE)

            if changed: write_text(opf_path, content)

        except Exception as e:
            _warn("guide repair failed", opf_path, e)


def _maybe_inject_guide(opf_content, opf_path, opf_dir):
    # If no <guide> exists, inject a minimal one with cover and text references
    # when they can be confidently inferred from the manifest and spine.
    #
    # The <guide> element is defined in EPUB2 (OPF 2.0 §2.6) and carried forward
    # as a DEPRECATED but valid optional element in EPUB3 (EPUB3 §4.5).
    # We only inject it for EPUB2 books — EPUB3 books use
    # <nav epub:type="landmarks"> instead, which inject_landmarks() handles.
    #
    # Only writes the OPF if a text reference can be found.
    try:
        # Gate on EPUB version: only inject <guide> for EPUB2
        ver_m = re.search(r'<package[^>]*\bversion="([^"]+)"', opf_content, re.IGNORECASE)
        epub_ver = (ver_m.group(1) if ver_m else "2.0").strip()
        if epub_ver.startswith("3"):
            return  # EPUB3 uses <nav epub:type="landmarks"> — inject_landmarks() handles it
        # Find cover href
        cover_href = None
        cover_m = re.search(
            r'<item\s[^>]*\bproperties="[^"]*cover-image[^"]*"[^>]*\bhref="([^"]+)"',
            opf_content, re.IGNORECASE)
        if not cover_m:
            cover_m = re.search(
                r'<meta\s[^>]*\bname="cover"[^>]*\bcontent="([^"]+)"',
                opf_content, re.IGNORECASE)
            if cover_m:
                cover_id = cover_m.group(1)
                href_m = re.search(
                    rf'<item\s[^>]*\bid="{re.escape(cover_id)}"[^>]*\bhref="([^"]+)"',
                    opf_content, re.IGNORECASE)
                if href_m: cover_href = href_m.group(1)
        else:
            # cover-image points to the image, not the cover HTML page —
            # find the first spine HTML file that contains this image
            img_href = cover_m.group(1)
            img_base = os.path.basename(img_href)
            for idref in re.findall(
                    r'<itemref\s[^>]*\bidref="([^"]+)"', opf_content, re.IGNORECASE):
                item_href_m = re.search(
                    rf'<item\s[^>]*\bid="{re.escape(idref)}"[^>]*\bhref="([^"]+)"',
                    opf_content, re.IGNORECASE)
                if not item_href_m: continue
                h = item_href_m.group(1)
                abs_h = os.path.normpath(os.path.join(opf_dir, h))
                if os.path.exists(abs_h):
                    try:
                        if img_base in read_text(abs_h):
                            cover_href = h
                            break
                    except Exception: pass

        # Find first linear spine item as the text start point
        text_href = None
        for idref in re.findall(
                r'<itemref\s[^>]*\bidref="([^"]+)"(?![^>]*linear="no")',
                opf_content, re.IGNORECASE):
            href_m2 = re.search(
                rf'<item\s[^>]*\bid="{re.escape(idref)}"[^>]*\bhref="([^"]+)"',
                opf_content, re.IGNORECASE)
            if href_m2:
                h = href_m2.group(1)
                if os.path.exists(os.path.normpath(os.path.join(opf_dir, h))):
                    if cover_href and os.path.basename(h) == os.path.basename(cover_href): continue
                    text_href = h
                    break

        if not text_href: return  # can't infer reliably — don't inject

        refs = []
        if cover_href: refs.append(f'  <reference type="cover" href="{cover_href}" title="Cover"/>')
        refs.append(f'  <reference type="text" href="{text_href}" title="Begin Reading"/>')

        guide_block = '<guide>\n' + '\n'.join(refs) + '\n</guide>'
        new_content = re.sub(
            r'(</spine>)', r'\1\n' + guide_block, opf_content,
            count=1, flags=re.IGNORECASE)
        write_if_changed(opf_path, opf_content, new_content)
    except Exception as e:
        _warn("guide injection failed", opf_path, e)


# ════════════════════════════════════════════════════════════════════════════════
# 5. Degenerate TOC rebuild
# ════════════════════════════════════════════════════════════════════════════════

def repair_degenerate_toc(temp_dir):
    # Detect and rebuild degenerate TOC documents (fewer than TOC_MIN_ENTRIES entries).
    #
    # A TOC with 0, 1, or 2 entries usually means the original TOC generation failed
    # or the book was exported with only a cover entry.  On Kobo devices, a
    # single-entry TOC disables the navigation drawer entirely.
    #
    # Repair strategy:
    # 1. Count entries in NCX and NAV.
    # 2. If either has fewer than TOC_MIN_ENTRIES:
    # a. Rebuild from h1/h2 heading scan (same as generate_epub3_nav).
    # b. If heading scan also yields too few (e.g. a picture book), leave as-is
    # and log a warning — a fabricated TOC would be worse.
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            opf = read_text(opf_path)

            # ── Check NCX ────────────────────────────────────────────────────
            ncx_match = re.search(
                r'<item\s[^>]*\bmedia-type="application/x-dtbncx\+xml"[^>]*\bhref="([^"]+)"',
                opf, re.IGNORECASE)
            if ncx_match:
                ncx_path = os.path.normpath(os.path.join(opf_dir, ncx_match.group(1)))
                if os.path.exists(ncx_path):
                    entries = _parse_ncx_navpoints(read_text(ncx_path))
                    if 0 < len(entries) < TOC_MIN_ENTRIES:
                        print(f"  Warning: NCX has only {len(entries)} entr"
                              f"{'y' if len(entries)==1 else 'ies'} in "
                              f"{os.path.basename(ncx_path)} — attempting rebuild",
                              file=sys.stderr)
                        _rebuild_ncx_from_headings(opf, opf_dir, ncx_path)

            # ── Check NAV ────────────────────────────────────────────────────
            nav_item_m = re.search(
                r'<item\s[^>]*\bproperties="[^"]*\bnav\b[^"]*"[^>]*\bhref="([^"]+)"',
                opf, re.IGNORECASE)
            if nav_item_m:
                nav_path = os.path.normpath(
                    os.path.join(opf_dir, nav_item_m.group(1)))
                if os.path.exists(nav_path):
                    nav_content = read_text(nav_path)
                    nav_entries = re.findall(
                        r'<li>\s*<a\b[^>]*\bhref="([^"#][^"]*)"', nav_content, re.IGNORECASE)
                    if 0 < len(nav_entries) < TOC_MIN_ENTRIES:
                        print(f"  Warning: NAV has only {len(nav_entries)} entr"
                              f"{'y' if len(nav_entries)==1 else 'ies'} — attempting rebuild",
                              file=sys.stderr)
                        _rebuild_nav_from_headings(opf, opf_dir, nav_path)

        except Exception as e:
            print(f"\n  Warning: degenerate TOC check failed for {opf_path}: {e}",
                  file=sys.stderr)


def _rebuild_ncx_from_headings(opf, opf_dir, ncx_path):
    # Replace a degenerate NCX with one built from heading scan.
    try:
        entries = _nav_entries_from_headings(opf, opf_dir, max_depth=TOC_HEADING_DEPTH)
        if len(entries) < TOC_MIN_ENTRIES: return  # heading scan also sparse — leave original
        # Build minimal NCX XML
        nav_points = []
        for idx, (label, href, depth) in enumerate(entries, 1):
            nav_points.append(
                f'  <navPoint id="np{idx}" playOrder="{idx}">\n'
                f'    <navLabel><text>{label}</text></navLabel>\n'
                f'    <content src="{href}"/>\n'
                f'  </navPoint>')
        # Preserve existing NCX metadata: docTitle, uid, depth
        existing = read_text(ncx_path)
        uid_m    = re.search(r'<meta\s[^>]*\bname="dtb:uid"[^>]*/>', existing, re.IGNORECASE)
        uid_tag  = uid_m.group(0) if uid_m else '<meta name="dtb:uid" content="uid"/>'
        ncx = (
            '<?xml version="1.0" encoding="utf-8"?>\n'
            '<!DOCTYPE ncx PUBLIC "-//NISO//DTD ncx 2005-1//EN"\n'
            '"http://www.daisy.org/z3986/2005/ncx-2005-1.dtd">\n'
            '<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">\n'
            '<head>\n'
            f'  {uid_tag}\n'
            f'  <meta name="dtb:depth" content="{min(2, TOC_HEADING_DEPTH)}"/>\n'
            '</head>\n'
            '<docTitle><text>Contents</text></docTitle>\n'
            '<navMap>\n'
            + '\n'.join(nav_points) +
            '\n</navMap>\n</ncx>'
        )
        write_text(ncx_path, _renumber_ncx_play_order(ncx))
    except Exception as e: print(f"\n  Warning: NCX rebuild failed: {e}", file=sys.stderr)


def _rebuild_nav_from_headings(opf, opf_dir, nav_path):
    # Replace a degenerate NAV toc with one built from heading scan.
    try:
        entries = _nav_entries_from_headings(opf, opf_dir, max_depth=TOC_HEADING_DEPTH)
        if len(entries) < TOC_MIN_ENTRIES: return  # heading scan also sparse — leave original
        lang        = _get_book_language(opf_dir) or 'en'
        nav_content = read_text(nav_path)
        # Build new <nav epub:type="toc"> block
        items = '\n'.join(
            f'  <li class="nav-depth-{depth}"><a href="{href}">{label}</a></li>'
            for label, href, depth in entries)
        new_toc = (
            f'<nav epub:type="toc" id="toc">\n'
            f'  <h2>Contents</h2>\n  <ol>\n{items}\n  </ol>\n</nav>'
        )
        # Replace existing toc nav block
        replaced = re.sub(
            r'<nav[^>]*epub:type="toc"[^>]*>.*?</nav>',
            new_toc, nav_content, count=1, flags=re.IGNORECASE | re.DOTALL)
        if replaced == nav_content:
            # No existing toc nav — insert before </body>
            replaced = re.sub(r'(</body>)', new_toc + r'\1', nav_content,
                              count=1, flags=re.IGNORECASE)
        write_text(nav_path, replaced)
    except Exception as e: print(f"\n  Warning: NAV rebuild failed: {e}", file=sys.stderr)


# ════════════════════════════════════════════════════════════════════════════════
# 6. Large HTML file warning
# ════════════════════════════════════════════════════════════════════════════════

def warn_large_html_files(temp_dir):
    # Warn about HTML files exceeding HTML_SPLIT_WARN_KB.
    #
    # Kindle hard-limits individual HTML files at ~300KB and early firmware at
    # 260KB; files above this threshold crash the renderer or are silently
    # truncated.  Kobo has a softer limit around 500KB.
    #
    # We detect and warn but do not split — splitting requires rewriting all
    # internal anchors, cross-document links, and the OPF spine, which is a
    # complex operation reserved for a dedicated split pass.
    # Returns a list of (path, size_kb) for each oversized file.
    oversized = []
    for html_path in find_html(temp_dir):
        try:
            size_kb = os.path.getsize(html_path) / 1024
            if size_kb > HTML_SPLIT_WARN_KB:
                oversized.append((html_path, size_kb))
                print(f"  Warning: HTML file {os.path.basename(html_path)} "
                      f"is {size_kb:.0f} KB (limit: {HTML_SPLIT_WARN_KB} KB) — "
                      f"may crash on Kindle devices", file=sys.stderr)
        except Exception: pass
    return oversized


# ════════════════════════════════════════════════════════════════════════════════
# 7. ZIP small-file optimization
# ════════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10f — HTML FEATURES (page-break, img dimensions, font units)
# ═══════════════════════════════════════════════════════════════════════════════
#
#  1. inject_page_break_css          — page-break hints for figures, tables, etc.
#  2. sync_image_dimensions          — update width/height attrs after resize
#  3. convert_font_size_units        — pt/px → em (respects user font settings)
#  4. (repair_opf_metadata extended) — dc:creator injection, year-only dc:date


# ════════════════════════════════════════════════════════════════════════════════
# 1. Page-break CSS injection
# ════════════════════════════════════════════════════════════════════════════════


def sync_image_dimensions(temp_dir):
    # Update (or add) width= and height= attributes on <img> tags to match the
    # actual pixel dimensions of the image files on disk.
    #
    # Why: process_images() resizes images but never updates the HTML attributes.
    # When width/height are wrong or absent, the reading system must decode the
    # image before it can lay out the page — causing visible reflow jitter on
    # Kobo and slow page turns on older Kindles.
    #
    # Strategy:
    # 1. Build a map of image-basename → (width, height) from files on disk.
    # 2. For each HTML file, find <img src="…"> tags.
    # 3. Set or replace the width= and height= attributes to match disk values.
    # 4. Uses original attribute values as a signal: if both are already
    # correct, skip (no write).
    #
    # Only runs if Pillow is available (needed to read dimensions).
    # Does NOT set width/height on <img> tags that use CSS-only sizing
    # (i.e. if style= already contains 'width' or 'height', leave those alone).
    if not HAS_PIL: return

    # ── Build dimension map: abs_path → (w, h) ───────────────────────────────
    # Keyed by normalised absolute path to avoid basename collisions (two images
    # in different subdirectories can share the same filename).  The lookup in
    # update_img() resolves src= relative to the HTML file's directory first,
    # then falls back to a basename scan only when the path is ambiguous.
    dim_map = {}          # abs_path → (w, h)
    basename_map = {}     # basename → list[abs_path]  (for fallback)
    for root, _, files in os.walk(temp_dir):
        for file in files:
            ext = os.path.splitext(file)[1].lower()
            if ext not in ('.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp'): continue
            fp = os.path.normpath(os.path.join(root, file))
            try:
                with Image.open(fp) as img:
                    dim_map[fp] = img.size  # (w, h)
                basename_map.setdefault(file.lower(), []).append(fp)
            except Exception: pass

    if not dim_map: return

    for html_path in find_html(temp_dir):
        try:
            html, changed = read_text(html_path), False
            html_dir = os.path.dirname(html_path)

            def update_img(m, _html_dir=html_dir):
                nonlocal changed
                attrs = m.group(1)   # clean attributes, no trailing slash
                # Skip if CSS sizing is already set inline
                if re.search(r'\bstyle="[^"]*\b(?:width|height)\b', attrs, re.IGNORECASE):
                    return m.group(0)
                src_m = re.search(r'\bsrc="([^"]+)"', attrs, re.IGNORECASE)
                if not src_m: return m.group(0)
                src      = src_m.group(1).split('?')[0]
                # Resolve to absolute path relative to containing HTML file
                abs_src  = os.path.normpath(os.path.join(_html_dir, src))
                dims     = dim_map.get(abs_src)
                if dims is None:
                    # Fallback: basename lookup — only use if unambiguous
                    candidates = basename_map.get(os.path.basename(src).lower(), [])
                    dims = dim_map.get(candidates[0]) if len(candidates) == 1 else None
                if not dims: return m.group(0)
                w, h = dims
                # Remove existing width= and height= (we'll re-add correct ones)
                new_attrs = re.sub(r'\s*\bwidth="[^"]*"', '', attrs, flags=re.IGNORECASE)
                new_attrs = re.sub(r'\s*\bheight="[^"]*"', '', new_attrs, flags=re.IGNORECASE)
                new_attrs = new_attrs.rstrip() + f' width="{w}" height="{h}"'
                changed = True
                return f'<img{new_attrs}/>'   # always emit self-closing

            new_html = _IMG_TAG_RE.sub(update_img, html)
            if changed: write_text(html_path, new_html)
        except Exception as e:
            _warn("image dimension sync failed", html_path, e)


# ════════════════════════════════════════════════════════════════════════════════
# 3. Font-size unit conversion: pt/px → em
# ════════════════════════════════════════════════════════════════════════════════

def _px_to_em(value: float) -> str:
    em = value / _PX_PER_EM
    return f'{em:.3f}'.rstrip('0').rstrip('.') + 'em'


def _pt_to_em(value: float) -> str:
    em = value / _PT_PER_EM
    return f'{em:.3f}'.rstrip('0').rstrip('.') + 'em'



def consolidate_identical_css(temp_dir):
    # Find CSS files with identical content and collapse them to a single shared
    # stylesheet.
    #
    # Why: some export tools (especially older InDesign scripts and certain Calibre
    # converters) generate one CSS file per HTML chapter, all containing identical
    # content.  This wastes disk space proportional to the chapter count and makes
    # CSS changes apply to only some chapters.
    #
    # Strategy:
    # 1. Hash every CSS file.
    # 2. Group by hash.
    # 3. For each group with 2+ members, keep the first (alphabetically), update
    # all HTML files that reference the others to point at the canonical one,
    # and delete the duplicates.
    # 4. Remove the now-orphaned manifest items.
    #
    # Skips CSS files that are already the same file (no-op).
    # Skips CSS files referenced from outside the EPUB (external URLs — shouldn't
    # exist but defensive).

    css_paths = find_css(temp_dir)
    if len(css_paths) < 2: return

    # ── 1. Hash all CSS files ─────────────────────────────────────────────────
    hash_to_paths = {}
    for cp in css_paths:
        try:
            h = hashlib.sha256(read_raw(cp)).hexdigest()
            hash_to_paths.setdefault(h, []).append(cp)
        except Exception: pass

    # ── 2. Find duplicate groups ──────────────────────────────────────────────
    groups = {h: sorted(paths) for h, paths in hash_to_paths.items()
              if len(paths) > 1}
    if not groups: return

    redirects = {}  # old_basename → canonical_basename (relative from same dir)
    to_delete  = []

    for h, paths in groups.items():
        canonical = paths[0]   # keep the first alphabetically
        for dup in paths[1:]:
            redirects[dup] = canonical
            to_delete.append(dup)
            print(f"\n  CSS dedup: {os.path.basename(dup)} → {os.path.basename(canonical)}",
                  file=sys.stderr)

    # ── 3. Rewrite HTML <link rel="stylesheet" href="..."> references ─────────
    for html_path in find_html(temp_dir):
        try:
            html, changed = read_text(html_path), False
            html_dir = os.path.dirname(html_path)

            def rewrite_link(m):
                nonlocal changed
                tag   = m.group(0)
                href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                if not href_m: return tag
                href     = href_m.group(1)
                abs_css  = os.path.normpath(os.path.join(html_dir, href))
                canonical = redirects.get(abs_css)
                if canonical is None: return tag
                new_href = os.path.relpath(canonical, html_dir).replace(os.sep, '/')
                changed  = True
                return tag.replace(href_m.group(0), f'href="{new_href}"')

            html = re.sub(
                r'<link\b[^>]*\brel=["\']stylesheet["\'][^>]*/?>',
                rewrite_link, html, flags=re.IGNORECASE)
            if changed: write_text(html_path, html)
        except Exception as e:
            print(f"\n  Warning: CSS redirect rewrite failed for {html_path}: {e}",
                  file=sys.stderr)

    # ── 4. Delete duplicate files ─────────────────────────────────────────────
    for dp in to_delete:
        try:
            os.remove(dp)
        except Exception: pass

    # ── 5. Remove orphaned manifest entries for deleted CSS files ─────────────
    # cleanup_manifest() at step 26 would catch these too, but doing it here
    # makes the dependency explicit and ensures correctness regardless of ordering.
    to_delete_set = set(os.path.normpath(p) for p in to_delete)
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            opf = read_text(opf_path)
            def remove_dup_css_item(m):
                tag    = m.group(0)
                href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                if not href_m: return tag
                abs_p  = os.path.normpath(os.path.join(opf_dir, href_m.group(1)))
                return '' if abs_p in to_delete_set else tag
            new_opf = re.sub(r'<item\s[^>]*/\s*>', remove_dup_css_item, opf, flags=re.IGNORECASE)
            write_if_changed(opf_path, opf, new_opf)
        except Exception as e:
            _warn("CSS dedup manifest cleanup failed", opf_path, e)



def deduplicate_inline_styles(temp_dir):
    # Extract repeated identical inline <style> blocks from HTML chapter files
    # and consolidate them into a single shared CSS file.
    #
    # Some export tools (Calibre, older InDesign scripts) emit one identical
    # <style>…</style> block in every chapter's <head>.  These can't be caught
    # by consolidate_identical_css (which only sees separate CSS files), but they
    # inflate every HTML file and make the CSS harder to modify.
    #
    # Strategy:
    # 1. Collect all inline <style> blocks from HTML files.
    # 2. Hash each block's content (after stripping whitespace for normalisation).
    # 3. If the same content appears in 3+ files, extract it to a new shared CSS
    #    file, add a <link> to each HTML file, and remove the inline block.
    # 4. Register the new CSS file in the OPF manifest.
    #
    # Threshold: 3+ occurrences (2 could be coincidence; 3+ is clearly systematic).

    html_paths = find_html(temp_dir)
    if len(html_paths) < 3: return

    _STYLE_BLOCK_RE = re.compile(
        r'(<style\b[^>]*>)(.*?)(</style>)', re.IGNORECASE | re.DOTALL)

    # ── 1. Collect inline style blocks per HTML file ───────────────────────────
    hash_to_content  = {}   # normalised_hash → canonical_css_text
    hash_to_files    = {}   # normalised_hash → list[html_abs_path]
    file_to_blocks   = {}   # html_abs_path   → list[(full_match, css_text, hash)]

    for html_path in html_paths:
        try:
            html = read_text(html_path)
            for m in _STYLE_BLOCK_RE.finditer(html):
                css_text = m.group(2)
                norm     = re.sub(r'\s+', ' ', css_text).strip()
                if len(norm) < 50: continue   # too short to bother extracting
                h = hashlib.sha256(norm.encode()).hexdigest()
                hash_to_content.setdefault(h, css_text)
                hash_to_files.setdefault(h, []).append(html_path)
                file_to_blocks.setdefault(html_path, []).append((m.group(0), css_text, h))
        except Exception: pass

    # ── 2. Find hashes that appear in 3+ files ────────────────────────────────
    repeated = {h for h, files in hash_to_files.items() if len(files) >= 3}
    if not repeated: return

    # ── 3. For each OPF, create a shared CSS file and register it ─────────────
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        css_dir = os.path.join(opf_dir, 'Styles')   # standard CSS location
        # Find actual CSS directory from existing files
        existing_css = find_css(temp_dir)
        if existing_css:
            css_dir = os.path.dirname(existing_css[0])
        os.makedirs(css_dir, exist_ok=True)

        for h in repeated:
            css_text   = hash_to_content[h]
            # Pick a filename that doesn't exist yet
            base_name  = f'shared_{h[:8]}.css'
            css_path   = os.path.join(css_dir, base_name)
            if os.path.exists(css_path): continue   # already extracted (multi-OPF edge case)

            write_text(css_path, css_text.strip() + '\n')

            # Register in OPF manifest
            opf = read_text(opf_path)
            opf_rel  = os.path.relpath(css_path, opf_dir).replace(os.sep, '/')
            new_item = (f'\n    <item id="shared-css-{h[:8]}" '
                        f'href="{opf_rel}" media-type="text/css"/>')
            opf = re.sub(r'(</manifest>)', new_item + r'\n  \1', opf,
                         count=1, flags=re.IGNORECASE)
            write_text(opf_path, opf)

            # ── 4. Rewrite each affected HTML file ────────────────────────────
            for html_path in hash_to_files[h]:
                try:
                    html      = read_text(html_path)
                    html_dir  = os.path.dirname(html_path)
                    link_href = os.path.relpath(css_path, html_dir).replace(os.sep, '/')
                    link_tag  = f'<link rel="stylesheet" href="{link_href}"/>'

                    # Remove all inline <style> blocks with this hash
                    def remove_block(m, _h=h):
                        css  = m.group(2)
                        norm = re.sub(r'\s+', ' ', css).strip()
                        bh   = hashlib.sha256(norm.encode()).hexdigest()
                        return '' if bh == _h else m.group(0)
                    new_html = _STYLE_BLOCK_RE.sub(remove_block, html)

                    # Insert the <link> in <head> if not already present
                    if link_href not in new_html:
                        new_html = re.sub(
                            r'(</head>)',
                            f'  {link_tag}\n\\1', new_html,
                            count=1, flags=re.IGNORECASE)

                    write_if_changed(html_path, html, new_html)
                except Exception as e:
                    _warn("inline style dedup rewrite failed", html_path, e)

            print(f"\n  Inline CSS dedup: extracted {len(hash_to_files[h])}× repeated "
                  f"block → {base_name}", file=sys.stderr)


# ════════════════════════════════════════════════════════════════════════════════
# 2. Detect (and optionally remove) empty or near-empty spine documents
# ════════════════════════════════════════════════════════════════════════════════


def _body_is_empty(html):
    # True if the HTML file has a body that contains no meaningful visible text
    # or images.  Allows: whitespace, <br/>, empty <p></p>, empty <div></div>.
    body_m = re.search(r'<body\b[^>]*>(.*?)</body>', html,
                       re.IGNORECASE | re.DOTALL)
    if not body_m: return False
    body = body_m.group(1)
    # Strip tags to get raw text content
    text_only = re.sub(r'<[^>]+>', '', body)
    # If there's an image tag anywhere in body → not empty
    if re.search(r'<img\b', body, re.IGNORECASE): return False
    return not bool(_MEANINGFUL_TEXT_RE.search(text_only))


def detect_empty_spine_documents(temp_dir):
    # Scan spine HTML documents for empty or near-empty body content.
    #
    # Empty documents (body contains only whitespace, <br>, empty blocks):
    # • Warn and remove from the spine (keep the file in the manifest so
    # cross-references survive — just mark linear="no").
    # • Common source: transition pages, placeholder chapters, bad splits.
    #
    # This is a conservative operation:
    # • We never delete the file itself (TOC entries may point to it).
    # • We only set linear="no" on the spine itemref, removing the page
    # from the reader's reading flow.
    # • A warning is always printed.
    for opf_path in find_opf(temp_dir):
        try:
            opf_dir = os.path.dirname(opf_path)
            opf     = read_text(opf_path)
            id_to_href = _parse_manifest_items(opf)

            # Build map: idref → resolved absolute path
            spine_idrefs = re.findall(
                r'<itemref\s[^>]*\bidref="([^"]+)"', opf, re.IGNORECASE)

            empty_idrefs = set()
            for idref in spine_idrefs:
                href = id_to_href.get(idref, '')
                abs_path = os.path.normpath(os.path.join(opf_dir, href))
                if not os.path.exists(abs_path): continue
                ext = os.path.splitext(abs_path)[1].lower()
                if ext not in ('.html', '.xhtml', '.htm'): continue
                try:
                    if _body_is_empty(read_text(abs_path)):
                        empty_idrefs.add(idref)
                        print(f"\n  Warning: empty spine document: {href} (idref='{idref}')"
                              " — setting linear=\"no\"", file=sys.stderr)
                except Exception: pass

            if not empty_idrefs: continue

            # Mark empty spine items as non-linear
            def mark_nonlinear(m):
                tag   = m.group(0)
                idref_m = re.search(r'\bidref="([^"]+)"', tag, re.IGNORECASE)
                if not idref_m or idref_m.group(1) not in empty_idrefs: return tag
                # Already non-linear?
                if re.search(r'\blinear="no"', tag, re.IGNORECASE): return tag
                # Replace or add linear="no"
                if re.search(r'\blinear=', tag, re.IGNORECASE):
                    return re.sub(r'\blinear="[^"]*"', 'linear="no"', tag,
                                  flags=re.IGNORECASE)
                return re.sub(r'(<itemref\b)', r'\1 linear="no"', tag,
                              flags=re.IGNORECASE)

            new_opf = re.sub(r'<itemref\b[^>]*/>', mark_nonlinear, opf,
                             flags=re.IGNORECASE)
            write_if_changed(opf_path, opf, new_opf)

        except Exception as e:
            print(f"\n  Warning: empty spine detection failed for {opf_path}: {e}",
                  file=sys.stderr)


# ════════════════════════════════════════════════════════════════════════════════
# 3. Preserve / upgrade series metadata
# ════════════════════════════════════════════════════════════════════════════════


def _normalize_series_name(name: str) -> str:
    # Conservative normalisation of a series name extracted from Calibre metadata.
    #
    # Rules applied (in order):
    # 1. Whitespace normalisation.
    # 2. Blank known placeholder values (000-No Series, Stand-Alone, Novel …).
    # 3. Strip trailing comma (common import artefact).
    # 4. Strip reading-order parentheticals:
    # "Darkover (Publication Order)"  → "Darkover"
    # "Foundation (Chronological Order)" → "Foundation"
    # 5. Strip inline reading-order suffixes after colon/dash:
    # "Shannara Universe: Chronological" → "Shannara Universe"
    # 6. Strip redundant "Series" suffix — ONLY when the remaining stem is:
    # • at least 4 characters long, AND
    # • not a generic word (adventure, anthology, saga, trilogy …).
    # "Alex Delaware Series" → "Alex Delaware"   (safe)
    # "Adventure Series"     → unchanged          (stem too generic)
    #
    # Deliberately NOT done:
    # • "The X" → "X"  (223 pairs exist but correct form varies per series)
    # • Collapsing sub-series (Star Trek: DS9 ≠ Star Trek: TNG)
    # • Any change that requires knowing the ground-truth series name
    name = _normalize_whitespace(name)
    if not name: return name

    # ── 2. Placeholder → blank ────────────────────────────────────────────────
    if _SERIES_PLACEHOLDER_RE.match(name): return ''

    # ── 3. Trailing comma ─────────────────────────────────────────────────────
    name = name.rstrip(',').strip()

    # ── 4. Reading-order parenthetical ───────────────────────────────────────
    name = _SERIES_ORDER_PAREN_RE.sub('', name).strip()

    # ── 5. Inline reading-order suffix ───────────────────────────────────────
    name = _SERIES_ORDER_SUFFIX_RE.sub('', name).strip()

    # ── 6. Redundant "Series" suffix ─────────────────────────────────────────
    candidate = _SERIES_SUFFIX_RE.sub('', name).strip()
    if (candidate != name
            and len(candidate) >= 4
            and candidate.lower() not in _SERIES_GENERIC_WORDS):
        name = candidate

    return name


def preserve_series_metadata(temp_dir):
    # Convert Calibre-proprietary series metadata to EPUB3 standard form.
    #
    # Calibre writes:
    # <meta name="calibre:series"       content="The Name Series"/>
    # <meta name="calibre:series_index" content="1"/>
    #
    # EPUB3 uses:
    # <meta property="belongs-to-collection" id="series-id">The Name Series</meta>
    # <meta refines="#series-id" property="collection-type">series</meta>
    # <meta refines="#series-id" property="group-position">1</meta>
    #
    # Strategy:
    # 1. Parse the OPF with ElementTree for reliability.
    # 2. Find calibre:series and calibre:series_index meta elements.
    # 3. If the EPUB3 belongs-to-collection is already present, leave it.
    # 4. Otherwise, inject the EPUB3 form and remove the Calibre metas.
    #
    # This preserves series info that would otherwise be silently destroyed by
    # the vendor-metadata stripping in remove_bloat/clean_metadata.
    #
    # Only runs for EPUB3 (version="3.0") books.  For EPUB2, keeps the Calibre
    # metas as-is since there is no standard alternative.
    for opf_path in find_opf(temp_dir):
        try:
            opf = read_text(opf_path)

            # Only process EPUB3
            ver_m = re.search(r'<package\b[^>]*\bversion="([^"]+)"', opf, re.IGNORECASE)
            if not ver_m or not ver_m.group(1).startswith('3'): continue

            # Already has belongs-to-collection?
            if re.search(r'property="belongs-to-collection"', opf, re.IGNORECASE): continue

            # Find calibre:series and calibre:series_index
            series_m = re.search(
                r'<meta\s[^>]*name="calibre:series"\s[^>]*content="([^"]+)"[^>]*/?>',
                opf, re.IGNORECASE)
            if not series_m:
                # Also try content before name
                series_m = re.search(
                    r'<meta\s[^>]*content="([^"]+)"\s[^>]*name="calibre:series"[^>]*/?>',
                    opf, re.IGNORECASE)
            if not series_m: continue

            series_name = _normalize_series_name(series_m.group(1).strip())
            if not series_name:
                # Placeholder value (Stand-Alone, 000-No Series, etc.) — remove
                # the Calibre metas and write nothing into belongs-to-collection.
                new_opf = re.sub(
                    r'\s*<meta\s[^>]*name="calibre:series(?:_index)?"[^>]*/?>',
                    '', opf, flags=re.IGNORECASE)
                write_if_changed(opf_path, opf, new_opf)
                continue

            index_m = re.search(
                r'<meta\s[^>]*name="calibre:series_index"\s[^>]*content="([^"]+)"[^>]*/?>',
                opf, re.IGNORECASE)
            if not index_m:
                index_m = re.search(
                    r'<meta\s[^>]*content="([^"]+)"\s[^>]*name="calibre:series_index"[^>]*/?>',
                    opf, re.IGNORECASE)
            series_index = index_m.group(1).strip() if index_m else '1'

            # Validate group-position: must be a positive number.
            # Calibre sometimes exports 0 or negative values for un-numbered entries.
            try:
                gp_val = float(series_index)
                if gp_val <= 0:
                    print(f"\n  Warning: series '{series_name}' has invalid group-position "
                          f"'{series_index}' (≤ 0) — setting to 1", file=sys.stderr)
                    series_index = '1'
                elif gp_val == int(gp_val):
                    series_index = str(int(gp_val))  # "1.0" → "1"
            except ValueError:
                print(f"\n  Warning: series '{series_name}' has non-numeric group-position "
                      f"'{series_index}' — setting to 1", file=sys.stderr)
                series_index = '1'

            # Build EPUB3 collection metadata block
            epub3_series = (
                f'\n    <meta property="belongs-to-collection" id="col-id-1">'
                f'{series_name}</meta>'
                f'\n    <meta refines="#col-id-1" property="collection-type">series</meta>'
                f'\n    <meta refines="#col-id-1" property="group-position">'
                f'{series_index}</meta>'
            )

            # Remove calibre:series* metas and insert EPUB3 form
            new_opf = re.sub(
                r'\s*<meta\s[^>]*name="calibre:series(?:_index)?"[^>]*/?>',
                '', opf, flags=re.IGNORECASE)

            # Insert before </metadata>
            new_opf = re.sub(
                r'(</metadata>)',
                epub3_series + r'\n  \1',
                new_opf, count=1, flags=re.IGNORECASE)

            if write_if_changed(opf_path, opf, new_opf):
                orig_name = series_m.group(1).strip()
                if orig_name != series_name:
                    print(f"\n  Series: '{orig_name}' → '{series_name}' #{series_index} → "
                          f"belongs-to-collection", file=sys.stderr)
                else:
                    print(f"\n  Series: '{series_name}' #{series_index} → "
                          f"belongs-to-collection", file=sys.stderr)

        except Exception as e:
            print(f"\n  Warning: series metadata conversion failed for {opf_path}: {e}",
                  file=sys.stderr)


# ════════════════════════════════════════════════════════════════════════════════
# 4. Remove attribute-only empty <span> and <div> elements
# ════════════════════════════════════════════════════════════════════════════════


def remove_empty_spans(temp_dir):
    apply_html_transforms(temp_dir,
        do_charset=False,
        do_lang=False,
        do_typography=False,
        do_alt_text=False,
        do_lazy_loading=False,
        do_normalize=False,
        do_sanitize_address=False,
        do_xml_space=False,
        do_strip_important_inline=False)

def add_xml_space_preserve(temp_dir):
    apply_html_transforms(temp_dir,
        do_charset=False,
        do_lang=False,
        do_typography=False,
        do_alt_text=False,
        do_lazy_loading=False,
        do_normalize=False,
        do_sanitize_address=False,
        do_remove_empty_spans=False,
        do_strip_important_inline=False)

def audit_orphaned_ids(temp_dir, verbose=False):
    # Find id= attributes in HTML that are never referenced by any href="#id" link
    # in any file in the book.
    #
    # This is a WARNING-only operation.  We never auto-delete orphaned IDs because:
    # • They may be referenced by JavaScript (which we strip, but the author
    # may have intended to keep them for external linking).
    # • They may be referenced from reading system features (bookmarks,
    # annotations, highlight anchors) that operate outside our visibility.
    # • TOC entries in the NCX/NAV may reference them — we check this too,
    # but external link sources cannot be enumerated.
    #
    # Output: prints a summary count.  With verbose=True, lists each orphaned id.
    #
    # Returns: set of orphaned id values (for use in other passes if needed).
    # ── 1. Collect all id= values across all HTML files ───────────────────────
    all_ids = {}   # id_value → filename
    for html_path in find_html(temp_dir):
        try:
            html = read_text(html_path)
            for id_val in re.findall(r'\bid="([^"]+)"', html, re.IGNORECASE):
                all_ids.setdefault(id_val, os.path.basename(html_path))
        except Exception: pass

    # ── 2. Collect all fragment references from HTML, NCX, and NAV ───────────
    referenced = set()
    # From HTML href="#..." and href="file.xhtml#..."
    for html_path in find_html(temp_dir):
        try:
            html = read_text(html_path)
            for href in re.findall(r'\bhref="[^"]*#([^"]+)"', html, re.IGNORECASE):
                referenced.add(href)
        except Exception: pass
    # From NCX
    for ncx_path in find_ncx(temp_dir):
        try:
            ncx = read_text(ncx_path)
            for href in re.findall(r'\bsrc="[^"]*#([^"]+)"', ncx, re.IGNORECASE):
                referenced.add(href)
        except Exception: pass
    # From NAV
    for nav_path in find_nav(temp_dir):
        try:
            nav = read_text(nav_path)
            for href in re.findall(r'\bhref="[^"]*#([^"]+)"', nav, re.IGNORECASE):
                referenced.add(href)
        except Exception: pass

    # ── 3. Find orphans ───────────────────────────────────────────────────────
    orphans = {id_val: fname for id_val, fname in all_ids.items()
               if id_val not in referenced}

    if orphans and verbose:
        print(f"\n  Orphaned id= attributes: {len(orphans)} "
              f"(referenced: {len(referenced)}, total: {len(all_ids)})",
              file=sys.stderr)
        for id_val, fname in sorted(orphans.items())[:20]:  # cap output
            print(f"    {fname}: #{id_val}", file=sys.stderr)
        if len(orphans) > 20: print(f"    … and {len(orphans) - 20} more", file=sys.stderr)

    return orphans


# ════════════════════════════════════════════════════════════════════════════════
# 7. Detect block elements inside <p> tags (warn only)
# ════════════════════════════════════════════════════════════════════════════════


def detect_block_in_paragraph(temp_dir, verbose=False):
    # Detect HTML block elements nested directly inside <p> tags.
    #
    # This violates the HTML content model and causes different rendering across
    # EPUB reading systems — some silently fix it (Kobo, Apple Books), others
    # render it incorrectly (some Kindle firmware, older Nooks).
    #
    # This is a WARNING-only operation.  Auto-repair would require a full HTML
    # parser and could alter the author's intent, so we surface the problem for
    # awareness without touching the file.
    #
    # Returns: dict of {filepath: count} for files with violations.
    violations = {}
    for html_path in find_html(temp_dir):
        try:
            html = read_text(html_path)
            matches = _OPEN_BLOCK_IN_P_RE.findall(html)
            if matches:
                violations[html_path] = len(matches)
                if verbose:
                    print(f"\n  Warning: block element(s) inside <p> in "
                          f"{os.path.basename(html_path)}: "
                          f"{len(matches)} instance(s) ({', '.join(set(matches))})",
                          file=sys.stderr)
        except Exception: pass
    if violations and not verbose:
        total = sum(violations.values())
        print(f"\n  Warning: {total} block-in-<p> violation(s) across "
              f"{len(violations)} file(s) — use --verbose for details",
              file=sys.stderr)
    return violations


# ════════════════════════════════════════════════════════════════════════════════
# 8. Cross-document id= uniqueness check (warn only)
# ════════════════════════════════════════════════════════════════════════════════

def check_cross_document_id_uniqueness(temp_dir, verbose=False):
    # Detect id= values that appear in more than one HTML file.
    #
    # Within a single file, duplicate IDs are repaired by repair_duplicate_ids().
    # Cross-document duplicates are not a validity error in EPUB (each HTML file
    # has its own ID namespace) but cause ambiguity when fragment links don't
    # specify a filename prefix (just "#id"), which some TOC generators do.
    #
    # This is a WARNING-only operation.
    #
    # Returns: dict of {id_value: [file1, file2, ...]} for colliding IDs.
    id_to_files = {}   # id_value → list of filenames
    for html_path in find_html(temp_dir):
        try:
            html = read_text(html_path)
            fname = os.path.basename(html_path)
            for id_val in set(re.findall(r'\bid="([^"]+)"', html, re.IGNORECASE)):
                id_to_files.setdefault(id_val, []).append(fname)
        except Exception: pass

    collisions = {k: v for k, v in id_to_files.items() if len(v) > 1}

    if collisions:
        if verbose:
            print(f"\n  Cross-document id= collisions: {len(collisions)}",
                  file=sys.stderr)
            for id_val, files in sorted(collisions.items())[:10]:
                print(f"    #{id_val} in: {', '.join(files)}", file=sys.stderr)
            if len(collisions) > 10:
                print(f"    … and {len(collisions) - 10} more", file=sys.stderr)
        else:
            print(f"\n  Warning: {len(collisions)} id= value(s) appear in multiple "
                  f"HTML files (cross-document) — use --verbose for details",
                  file=sys.stderr)

    return collisions


# ════════════════════════════════════════════════════════════════════════════════
# 9. TOC label vs heading text drift detection (warn only)
# ════════════════════════════════════════════════════════════════════════════════

def detect_toc_heading_drift(temp_dir, verbose=False):
    # Compare NAV/NCX chapter labels against the actual <h1>/<h2> text in the
    # HTML documents they point to.
    #
    # "Drift" means the TOC label and the heading text are significantly different
    # — a common problem in books that were edited after initial conversion.
    #
    # Matching strategy:
    # • Normalize both strings (strip HTML tags, collapse whitespace, lowercase).
    # • If the normalized edit-distance ratio is below 0.6, flag as drift.
    # (We use a simple token-overlap ratio, not full Levenshtein, to keep the
    # implementation dependency-free.)
    #
    # This is a WARNING-only operation.  Auto-repair would require choosing which
    # version (TOC or heading) is "correct", which is an editorial decision.
    #
    # Returns: list of (toc_label, heading_text, href) drift tuples.
    def _normalize_label(text):
        text = re.sub(r'<[^>]+>', '', text)   # strip tags
        text = re.sub(r'\s+', ' ', text).strip().lower()
        # Strip leading "chapter N" / "part N" prefixes for comparison
        text = re.sub(r'^(?:chapter|part|section|appendix)\s+\w+\W*', '', text)
        return text

    def _similarity(a, b):
        # Token overlap ratio in [0, 1].
        if not a or not b: return 0.0
        ta, tb = set(a.split()), set(b.split())
        if not ta or not tb: return 0.0
        return len(ta & tb) / max(len(ta), len(tb))

    drifts = []

    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        # ── Try NAV first, then NCX ──────────────────────────────────────────
        nav_entries = []

        # From NAV
        for nav_path in find_nav(temp_dir):
            try:
                nav = read_text(nav_path)
                for label, href in re.findall(
                        r'<a\s[^>]*href="([^"#"]+)(?:#[^"]*)?"[^>]*>(.*?)</a>',
                        nav, re.IGNORECASE | re.DOTALL):
                    nav_entries.append((_normalize_label(href),
                                        _normalize_label(label), href, label))
            except Exception: pass

        # From NCX if no NAV
        if not nav_entries:
            for ncx_path in find_ncx(temp_dir):
                try:
                    ncx = read_text(ncx_path)
                    for href, label in re.findall(
                            r'<content\s[^>]*src="([^"]+)".*?'
                            r'<text>(.*?)</text>',
                            ncx, re.IGNORECASE | re.DOTALL):
                        nav_entries.append((_normalize_label(href),
                                            _normalize_label(label), href, label))
                except Exception: pass

        # ── Compare each entry against actual heading ────────────────────────
        for _href_key, nav_label, href, raw_label in nav_entries:
            # Resolve href to a file
            file_part = href.split('#')[0]
            if not file_part: continue
            abs_html = os.path.normpath(os.path.join(opf_dir, file_part))
            if not os.path.exists(abs_html): continue
            try:
                html = read_text(abs_html)
                # Find first h1 or h2
                hm = re.search(r'<h[12]\b[^>]*>(.*?)</h[12]>',
                               html, re.IGNORECASE | re.DOTALL)
                if not hm: continue
                heading_text = _normalize_label(hm.group(1))
                if not heading_text or not nav_label: continue
                sim = _similarity(nav_label, heading_text)
                if sim < 0.55:   # threshold: less than 55% token overlap
                    drifts.append((raw_label.strip(), hm.group(1).strip(), href))
                    if verbose:
                        print(f"\n  TOC drift ({sim:.0%} match): "
                              f"TOC='{raw_label.strip()[:60]}' "
                              f"vs heading='{hm.group(1).strip()[:60]}'",
                              file=sys.stderr)
            except Exception: pass

    if drifts and not verbose:
        print(f"\n  Warning: {len(drifts)} TOC label(s) differ significantly "
              f"from heading text — use --verbose for details", file=sys.stderr)

    return drifts

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 11 — IMAGES
# ═══════════════════════════════════════════════════════════════════════════════

def remove_unused_images(temp_dir):
    # Use OPF-relative paths (not just basenames) to identify referenced images.
    # Basename-only matching causes false collisions when two subdirectories each
    # have e.g. "cover.jpg" — one could be deleted while the other is kept.
    opf_paths = find_opf(temp_dir)
    opf_dir   = os.path.dirname(opf_paths[0]) if opf_paths else temp_dir

    html_content = collect_html_text(temp_dir)
    referenced_in_html = set()
    for m in re.finditer(
            r'(?:src|href)="([^"]+\.(?:jpg|jpeg|png|gif|svg|webp))"',
            html_content, re.IGNORECASE):
        referenced_in_html.add(m.group(1).split('?')[0])

    css_referenced = set()
    for css_path in find_css(temp_dir):
        try:
            css = read_text(css_path)
            css_dir = os.path.dirname(css_path)
            for url in re.findall(r'url\(["\']?([^"\')\s]+)["\']?\)', css, re.IGNORECASE):
                # Resolve relative to CSS file so we can normalise to OPF-relative path
                abs_url = os.path.normpath(os.path.join(css_dir, url))
                try:
                    css_referenced.add(os.path.relpath(abs_url, opf_dir).replace(os.sep, '/'))
                except ValueError:
                    css_referenced.add(os.path.basename(url))
        except Exception: pass

    opf_referenced_abs = set()  # absolute paths declared in OPF manifest
    for opf_path in opf_paths:
        opf_d = os.path.dirname(opf_path)
        try:
            for href in re.findall(
                    r'<item\s[^>]*\bhref="([^"]+)"',
                    read_text(opf_path), re.IGNORECASE):
                ext = os.path.splitext(href)[1].lower()
                if ext in ('.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp'):
                    opf_referenced_abs.add(
                        os.path.normpath(os.path.join(opf_d, href)))
        except Exception: pass

    # Build set of safe absolute paths
    safe_abs = set(opf_referenced_abs)
    for rel in referenced_in_html | css_referenced:
        # Try to resolve relative paths from OPF dir
        candidate = os.path.normpath(os.path.join(opf_dir, rel))
        safe_abs.add(candidate)
        # Also add by basename as a fallback for ambiguous references
        safe_abs.add(rel)  # bare basename refs

    # Fallback: also collect safe basenames for references we can't fully resolve
    safe_basenames = {os.path.basename(p) for p in safe_abs}
    # Add basenames directly from HTML references
    for href in referenced_in_html:
        safe_basenames.add(os.path.basename(href.split('?')[0]))

    removed = set()
    for root, _, files in os.walk(temp_dir):
        for file in files:
            if not file.lower().endswith(IMAGE_EXTENSIONS_WITH_SVG): continue
            full = os.path.join(root, file)
            # Check by absolute path first (precise), then basename (fallback)
            if full in safe_abs or os.path.basename(full) in safe_basenames:
                continue
            try:
                os.remove(full); removed.add(full)
            except Exception: pass
    if removed:
        for opf_path in opf_paths:
            opf_d = os.path.dirname(opf_path)
            try:
                content = read_text(opf_path)
                def check_removed(match):
                    tag    = match.group(0)
                    href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                    if not href_m: return tag
                    abs_path = os.path.normpath(os.path.join(opf_d, href_m.group(1)))
                    return '' if abs_path in removed else tag
                content = re.sub(r'<item\s[^>]*/\s*>', check_removed, content, flags=re.IGNORECASE)
                content = re.sub(
                    r'<item\s[^>]*>(?:</item>)?', check_removed, content, flags=re.IGNORECASE)
                write_text(opf_path, content)
            except Exception: pass


def deduplicate_images(temp_dir):
    # Find image files with identical content (by BLAKE2b hash) and consolidate
    # them: keep the first (by sort order), rewrite all HTML/CSS/OPF references
    # to point at the canonical copy, and delete the duplicates.
    #
    # Uses BLAKE2b (digest_size=16) — 3-4x faster than SHA-256 for this use case
    # with equivalent collision resistance for deduplication.
    #
    # Safe: never deletes until references are already rewritten.
    ext_set = frozenset(IMAGE_EXTENSIONS_WITH_SVG)

    # ── 1. Hash all images ───────────────────────────────────────────────────
    hash_to_paths: dict[str, list[str]] = {}
    for root, _, files in os.walk(temp_dir):
        for fname in sorted(files):  # sort for deterministic canonical selection
            if not fname.lower().endswith(tuple(ext_set)): continue
            fp = os.path.join(root, fname)
            try:
                h = hashlib.blake2b(read_raw(fp), digest_size=16).hexdigest()
                hash_to_paths.setdefault(h, []).append(fp)
            except Exception: pass

    groups = {h: paths for h, paths in hash_to_paths.items() if len(paths) > 1}
    if not groups: return

    # ── 2. Build redirect map: duplicate_abs_path → canonical_abs_path ──────
    redirects: dict[str, str] = {}
    for h, paths in groups.items():
        canonical = paths[0]
        for dup in paths[1:]:
            redirects[dup] = canonical
            print(f"\n  Image dedup: {os.path.basename(dup)} → {os.path.basename(canonical)}",
                  file=sys.stderr)

    # ── 3. Rewrite references (OPF, HTML, CSS) ──────────────────────────────
    def _remap(old_abs: str, base_dir: str, canonical_abs: str) -> str:
        return os.path.relpath(canonical_abs, base_dir).replace(os.sep, '/')

    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            content = read_text(opf_path)
            changed = False
            def fix_opf_img(m):
                nonlocal changed
                tag    = m.group(0)
                href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                if not href_m: return tag
                abs_p = os.path.normpath(os.path.join(opf_dir, href_m.group(1)))
                canon = redirects.get(abs_p)
                if canon:
                    changed = True
                    new_rel = _remap(abs_p, opf_dir, canon)
                    return tag.replace(href_m.group(0), f'href="{new_rel}"')
                return tag
            content = re.sub(r'<item\s[^>]*/\s*>', fix_opf_img, content, flags=re.IGNORECASE)
            if changed: write_text(opf_path, content)
        except Exception as e:
            _warn("image dedup OPF update failed", opf_path, e)

    for html_path in find_html(temp_dir):
        html_dir = os.path.dirname(html_path)
        try:
            html = read_text(html_path)
            changed = False
            def fix_html_img(m):
                nonlocal changed
                attr, href = m.group(1), m.group(2)
                abs_p = os.path.normpath(os.path.join(html_dir, href))
                canon = redirects.get(abs_p)
                if canon:
                    changed = True
                    return f'{attr}="{_remap(abs_p, html_dir, canon)}"'
                return m.group(0)
            html = re.sub(r'\b(src|href)="([^"]*)"', fix_html_img, html, flags=re.IGNORECASE)
            if changed: write_text(html_path, html)
        except Exception as e:
            _warn("image dedup HTML update failed", html_path, e)

    for css_path in find_css(temp_dir):
        css_dir = os.path.dirname(css_path)
        try:
            css = read_text(css_path)
            changed = False
            def fix_css_img(m):
                nonlocal changed
                url = m.group(1).strip('\'"')
                abs_p = os.path.normpath(os.path.join(css_dir, url))
                canon = redirects.get(abs_p)
                if canon:
                    changed = True
                    return f'url("{_remap(abs_p, css_dir, canon)}")'
                return m.group(0)
            css = re.sub(r'url\(["\']?([^"\')\s]+)["\']?\)', fix_css_img, css,
                         flags=re.IGNORECASE)
            if changed: write_text(css_path, css)
        except Exception as e:
            _warn("image dedup CSS update failed", css_path, e)

    # ── 4. Delete duplicates after all references are rewritten ─────────────
    for dup_abs in redirects:
        try:
            os.remove(dup_abs)
        except OSError: pass

def update_manifest_media_types(temp_dir, renamed_files):
    if not renamed_files: return
    for opf_path in find_opf(temp_dir):
        try:
            content = read_text(opf_path)
            for old, new in renamed_files.items():
                content = content.replace(old, new)
                content = re.sub(
                    rf'(href="[^"]*{re.escape(new)}"[^>]*media-type=")image/png(")',
                    r'\1image/jpeg\2', content)
                content = re.sub(
                    rf'(media-type=")image/png("[^>]*href="[^"]*{re.escape(new)}")'
                    , r'\1image/jpeg\2', content)
            write_text(opf_path, content)
        except Exception as e:
            _warn("OPF media-type update failed", exc=e)

def update_html_image_references(temp_dir, renamed_files):
    if not renamed_files: return
    for html_path in find_html(temp_dir):
        try:
            content, changed = read_text(html_path), False
            for old, new in renamed_files.items():
                if old in content: content = content.replace(old, new); changed = True
            if changed: write_text(html_path, content)
        except Exception: pass

def _strip_exif(img):
    # Return a copy of img with all EXIF/metadata stripped.
    #
    # Special cases:
    # • P (palette) mode: Image.new('P') creates a blank palette, so we must
    # explicitly copy the palette and transparency after putdata().
    # Without this, converting the returned image to RGB maps all pixels to
    # index 0 (black), corrupting palette images like GIFs.
    # • All other modes: create a blank image of the same mode and copy pixels.
    # This drops all EXIF/ICC/XMP blobs that PIL stores in img.info.
    # img.copy() preserves pixels, palette (for P mode), and img.info.
    # We then selectively clear metadata blobs but keep 'transparency' which
    # is needed for correct palette→RGBA conversion of transparent GIFs.
    clean = img.copy()
    keep_keys = {'transparency'}
    for key in list(clean.info.keys()):
        if key not in keep_keys: del clean.info[key]
    return clean


def _save_png_optimized(img, path):
    # Save a PNG with optional palette quantization for large opaque-or-palette images.
    # Quantization reduces colour depth to 256 colours — imperceptible on e-ink screens
    # but can cut file size 30–80% for screenshots and diagrams.
    # Only quantizes if the file would be >= PNG_QUANTIZE_MIN_KB after a baseline save.
    # Try quantization only for images that don't need full 24-bit colour
    # (i.e. already palette mode, or RGBA/RGB that are large enough to benefit)
    try:
        size_estimate = img.size[0] * img.size[1] * len(img.getbands())
        if size_estimate > PNG_QUANTIZE_MIN_KB * 1024:
            if img.mode in ('RGBA', 'RGB', 'P', 'L'):
                quantized = img.quantize(
                    colors=PNG_QUANTIZE_COLORS, method=Image.Quantize.MEDIANCUT)
                quantized.save(path, 'PNG', optimize=True)
                return
    except Exception: pass  # quantization failed — fall through to normal save
    img.save(path, 'PNG', optimize=True)


def process_images(temp_dir):
    # Process all images in temp_dir:
    # • Resize proportionally if either dimension exceeds MAX_SIZE (no upscaling).
    # • Strip all EXIF / embedded metadata.
    # • Convert CMYK JPEGs to RGB before saving.
    # • Re-save JPEGs at JPEG_QUALITY with progressive encoding.
    # • Optimise PNGs with optional palette quantization.
    # • Convert WebP → JPEG (no alpha) or PNG (with alpha)
    # • Convert GIF → PNG (static) or keep as-is if animated
    # Runs on every image regardless of file size.
    # Returns dict of {old_filename: new_filename} for any renamed files.
    if not HAS_PIL: return {}
    renamed = {}
    for root, _, files in os.walk(temp_dir):
        for file in files:
            ext_lower = os.path.splitext(file)[1].lower()
            if ext_lower not in IMAGE_EXTENSIONS_ALL: continue
            fp = os.path.join(root, file)
            try:
                with Image.open(fp) as img:

                    # ── Detect animated GIF — leave untouched ────────────────
                    is_gif = ext_lower == '.gif'
                    if is_gif:
                        try:
                            img.seek(1)         # frame 1 exists → animated
                            continue            # skip animated GIFs entirely
                        except EOFError: img.seek(0)         # only one frame → static GIF

                    # ── CMYK → RGB ───────────────────────────────────────────
                    if img.mode == 'CMYK': img = img.convert('RGB')

                    # ── Resize if oversized ──────────────────────────────────
                    img.thumbnail(MAX_SIZE, Image.Resampling.LANCZOS)

                    # ── Strip EXIF / metadata ────────────────────────────────
                    img = _strip_exif(img)

                    is_png  = ext_lower == '.png'
                    is_webp = ext_lower == '.webp'
                    has_alpha = (img.mode in ('RGBA', 'LA') or
                                 (img.mode == 'P' and
                                  img.info.get('transparency') is not None))

                    if is_gif:
                        # Static GIF → PNG (better compression, wider support)
                        nf  = os.path.splitext(file)[0] + '.png'
                        out = os.path.join(root, nf)
                        if img.mode == 'P': img = img.convert('RGBA' if has_alpha else 'RGB')
                        _save_png_optimized(img, out)
                        os.remove(fp)
                        renamed[file] = nf

                    elif is_webp:
                        if has_alpha:
                            nf  = os.path.splitext(file)[0] + '.png'
                            out = os.path.join(root, nf)
                            if img.mode not in ('RGBA', 'LA'): img = img.convert('RGBA')
                            _save_png_optimized(img, out)
                        else:
                            nf  = os.path.splitext(file)[0] + '.jpg'
                            out = os.path.join(root, nf)
                            img.convert('RGB').save(out, 'JPEG', optimize=True,
                                                    quality=JPEG_QUALITY,
                                                    progressive=JPEG_PROGRESSIVE)
                        os.remove(fp)
                        renamed[file] = nf

                    elif is_png:
                        if has_alpha: _save_png_optimized(img, fp)
                        else:
                            nf  = os.path.splitext(file)[0] + '.jpg'
                            out = os.path.join(root, nf)
                            img.convert('RGB').save(out, 'JPEG', optimize=True,
                                                    quality=JPEG_QUALITY,
                                                    progressive=JPEG_PROGRESSIVE)
                            os.remove(fp)
                            renamed[file] = nf

                    else:
                        # JPEG (and any non-PNG/WebP/GIF raster)
                        if img.mode in ('RGBA', 'P', 'LA'): img = img.convert('RGB')
                        img.save(fp, 'JPEG', optimize=True,
                                 quality=JPEG_QUALITY,
                                 progressive=JPEG_PROGRESSIVE)

            except Exception as e: print(f"\n  Warning: skipping image {fp}: {e}", file=sys.stderr)
    return renamed


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 11b — DATA-URI IMAGE EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════
#
# Some tools embed images as base64 data: URIs in CSS background-image or HTML
# img src attributes.  These defeat our image optimizer (process_images never
# sees them as files), can't be deduplicated, and inflate text file sizes.
# We extract them to real files, add them to the OPF manifest, and rewrite the
# references — so subsequent image processing, deduplication and compression
# all work normally.


def _extract_data_uris(content, base_dir, opf_dir, prefix, existing_names):
    # Replace data: URI occurrences in `content` with file references.
    # Saves extracted image files into `base_dir`.
    # Returns (new_content, list_of_(filename, rel_path, mime_type) for manifest addition).
    # Uses unique sentinel tokens to avoid corrupting other file content.
    new_items = []
    counter   = [0]
    sentinel_map = {}  # sentinel_token -> (fname, rel_placeholder)

    def replace_uri(m):
        mime_raw = m.group(1).lower().strip()
        b64_data = re.sub(r'\s', '', m.group(2))
        ext      = _MIME_TO_EXT.get(mime_raw, '.bin')
        try:
            raw = base64.b64decode(b64_data)
        except Exception: return m.group(0)  # malformed base64 — leave alone

        # Generate a unique filename based on content hash
        digest = hashlib.md5(raw).hexdigest()[:8]
        fname  = f'{prefix}_{digest}{ext}'
        out_path = os.path.join(base_dir, fname)
        if fname not in existing_names:
            try:
                with open(out_path, 'wb') as fh: fh.write(raw)
                existing_names.add(fname)
                mime_norm = mime_raw if mime_raw != 'image/jpg' else 'image/jpeg'
                new_items.append((fname, mime_norm))
            except Exception: return m.group(0)

        # Use a unique sentinel token — never appears in real content
        token = f'\x00DATAURI_{counter[0]}\x00'
        counter[0] += 1
        sentinel_map[token] = fname
        return token

    new_content = _DATA_URI_RE.sub(replace_uri, content)
    return new_content, new_items, sentinel_map


def _add_items_to_manifest(opf_path, new_items, images_dir_rel):
    # Add a list of (filename, mime_type) items to the OPF manifest.
    # images_dir_rel is the path from OPF dir to the images directory.
    if not new_items: return
    try:
        content = read_text(opf_path)
        additions = []
        for fname, mime in new_items:
            item_id  = re.sub(r'[^a-zA-Z0-9_-]', '_', os.path.splitext(fname)[0])
            href     = (images_dir_rel + '/' + fname).lstrip('/')
            tag      = f'<item id="{item_id}" href="{href}" media-type="{mime}"/>'
            # Don't add if href already present in manifest
            if f'href="{href}"' not in content: additions.append(tag)
        if additions:
            # Insert before </manifest>
            new_content = re.sub(
                r'(</manifest>)',
                '\n  '.join(additions) + r'\n\1',
                content, count=1, flags=re.IGNORECASE
            )
            write_text(opf_path, new_content)
    except Exception as e:
        _warn("manifest update failed", opf_path, e)


def extract_datauri_images(temp_dir):
    # Extract base64 data: URI images embedded in CSS and HTML into real image files.
    #
    # Why: tools like Calibre, some Word converters, and web-clippers frequently
    # embed images as data: URIs.  This means:
    # • process_images() never sees them — no compression, no resize
    # • They inflate CSS/HTML file sizes by ~33% (base64 overhead)
    # • Identical images in different files can't be deduplicated
    # • The OPF manifest has no record of them
    #
    # After extraction:
    # • Files land in an 'extracted_images/' subfolder next to the OPF
    # • OPF manifest entries are added for each extracted file
    # • process_images() and remove_unused_images() work on them normally
    #
    # Handles: CSS background-image, HTML img src, HTML srcset.
    # Does NOT touch data: URIs in SVG files (they are often intentional inlines).
    opf_paths = find_opf(temp_dir)
    if not opf_paths: return

    opf_path = opf_paths[0]
    opf_dir  = os.path.dirname(opf_path)

    # Put extracted images in a subfolder next to the OPF
    images_dir = os.path.join(opf_dir, 'extracted_images')
    os.makedirs(images_dir, exist_ok=True)
    existing_names = set(os.listdir(images_dir))

    all_new_items = []

    # ── 1. Extract from CSS files ─────────────────────────────────────────────
    for css_path in find_css(temp_dir):
        try:
            css = read_text(css_path)
            if 'data:image' not in css.lower(): continue
            new_css, new_items, sentinel_map = _extract_data_uris(
                css, images_dir, opf_dir, 'css_img', existing_names)
            if new_items:
                # Replace sentinel tokens → correct relative paths from CSS dir
                css_dir = os.path.dirname(css_path)
                for token, fname in sentinel_map.items():
                    rel = os.path.relpath(
                        os.path.join(images_dir, fname), css_dir
                    ).replace(os.sep, '/')
                    new_css = new_css.replace(token, rel)
                write_text(css_path, new_css)
                all_new_items.extend(new_items)
        except Exception as e:
            print(f"\n  Warning: CSS data-URI extraction failed for {css_path}: {e}",
                  file=sys.stderr)

    # ── 2. Extract from HTML files ────────────────────────────────────────────
    for html_path in find_html(temp_dir):
        try:
            html = read_text(html_path)
            if 'data:image' not in html.lower(): continue
            new_html, new_items, sentinel_map = _extract_data_uris(
                html, images_dir, opf_dir, 'html_img', existing_names)
            if new_items:
                html_dir = os.path.dirname(html_path)
                for token, fname in sentinel_map.items():
                    rel = os.path.relpath(
                        os.path.join(images_dir, fname), html_dir
                    ).replace(os.sep, '/')
                    new_html = new_html.replace(token, rel)
                write_text(html_path, new_html)
                all_new_items.extend(new_items)
        except Exception as e:
            print(f"\n  Warning: HTML data-URI extraction failed for {html_path}: {e}",
                  file=sys.stderr)

    # ── 3. Update OPF manifest ────────────────────────────────────────────────
    if all_new_items:
        images_dir_rel = os.path.relpath(images_dir, opf_dir).replace(os.sep, '/')
        # Deduplicate (same file might be referenced multiple times)
        seen = set()
        unique_items = []
        for item in all_new_items:
            if item[0] not in seen:
                seen.add(item[0])
                unique_items.append(item)
        _add_items_to_manifest(opf_path, unique_items, images_dir_rel)
        print(f"\n  Extracted {len(unique_items)} data-URI image(s) to files",
              file=sys.stderr)

    # ── 4. Remove empty extracted_images dir if nothing was extracted ─────────
    try:
        if not os.listdir(images_dir): os.rmdir(images_dir)
    except Exception: pass

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 12 — MINIFICATION
# ═══════════════════════════════════════════════════════════════════════════════

def minify_html(content):
    # Minify HTML/XHTML while preserving whitespace inside elements where it is
    # semantically significant: <pre>, <code>, <textarea>, and <style>.
    #
    # Strategy: extract all such blocks → replace with NUL-delimited placeholders
    # → minify the remaining markup → restore originals verbatim.
    # Also preserves conditional comments (<!--[if …]).
    #
    # <style> blocks are stashed so that:
    #   a) CSS comments inside <style> are not hit by the HTML comment stripper
    #   b) multi-line CSS selectors are not collapsed by the inter-tag whitespace pass
    # <textarea> is stashed because it is a user-content element whose whitespace
    # is preserved in the DOM (pre-wrap behaviour).

    # ── 1. Extract and stash whitespace-sensitive blocks ─────────────────────
    _STASH_RE = re.compile(
        r'(<(?:pre|code|textarea|style)\b[^>]*>.*?</(?:pre|code|textarea|style)>)',
        re.IGNORECASE | re.DOTALL)
    stash   = {}
    counter = [0]
    def stash_block(m):
        key = f'\x00BLOCK{counter[0]}\x00'
        stash[key] = m.group(1)
        counter[0] += 1
        return key
    content = _STASH_RE.sub(stash_block, content)

    # ── 2. Strip non-conditional HTML comments ────────────────────────────────
    content = re.sub(r'<!--(?!\[if).*?-->', '', content, flags=re.DOTALL)

    # ── 3. Collapse inter-element whitespace (but not inside text nodes) ──────
    #    Replace runs of 2+ whitespace chars between tags with a single space.
    content = re.sub(r'>\s{2,}<', '> <', content)
    content = '\n'.join(line.strip() for line in content.splitlines())
    content = re.sub(r'\n{2,}', '\n', content)

    # ── 4. Restore stashed blocks verbatim ───────────────────────────────────
    for key, original in stash.items(): content = content.replace(key, original)

    return content.strip()

def minify_css(content):
    # Minify CSS while correctly handling @media/@supports rules.
    #
    # Bug fix: the previous re.sub(r'\\s+', ' ') collapsed the mandatory
    # space inside `@media screen and (min-width:600px)` → producing the invalid
    # `@media screen and(min-width:600px)`.
    #
    # Strategy: strip comments, then use a targeted pass that preserves the
    # required space after 'and', 'not', 'only' in media query preludes, and
    # after the closing paren in `@media (…) {`.
    # ── 1. Strip CSS comments ─────────────────────────────────────────────────
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

    # ── 2. Collapse all whitespace to single spaces ───────────────────────────
    content = re.sub(r'\s+', ' ', content)

    # ── 3. Strip spaces around structural punctuation ────────────────────────
    #    Do NOT strip space before '(' in @media — it is required:
    #    @media (max-width:600px)  →  valid
    #    @media(max-width:600px)   →  INVALID on some readers
    content = re.sub(r'\s*([{}:;,>+~])\s*', r'\1', content)

    # ── 4. Restore mandatory spaces in @media / @supports preludes ────────────
    #    After keywords: and, not, only, or — these are logical operators.
    content = re.sub(r'\b(and|not|only|or)\(', r'\1 (', content, flags=re.IGNORECASE)
    #    Before '(' after a media type word like 'screen', 'print', 'all':
    content = re.sub(r'(\bscreen|\bprint|\ball|\bspeech)\(', r'\1 (', content,
                     flags=re.IGNORECASE)

    # ── 5. Remove trailing semicolon before closing brace ────────────────────
    # Single-pass handles all nesting depths (};}} ;}} ;} etc.)
    content = re.sub(r';(\s*})', r'\1', content)

    return content.strip()

def minify_xml(content):
    content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
    content = '\n'.join(line.strip() for line in content.splitlines())
    return re.sub(r'\n{2,}', '\n', content).strip()

def minify_text_files(temp_dir):
    for root, _, files in os.walk(temp_dir):
        for file in files:
            ext       = os.path.splitext(file)[1].lower()
            file_path = os.path.join(root, file)
            if ext not in MINIFY_EXTENSIONS: continue
            try:
                content = read_text(file_path)
                if ext in ('.html', '.xhtml', '.htm'): fn = minify_html
                elif ext == '.css':                    fn = minify_css
                else:                                  fn = minify_xml
                write_text(file_path, fn(content))
            except Exception as e:
                _warn("could not minify", file_path, e)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 13 — STRUCTURAL AUTO-REPAIR & VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def repair_mimetype(temp_dir):
    mt_path = os.path.join(temp_dir, 'mimetype')
    try:
        correct = b'application/epub+zip'
        if not os.path.exists(mt_path) or read_raw(mt_path) != correct:
            with open(mt_path, 'wb') as f: f.write(correct)
    except Exception as e: print(f"\n  Warning: mimetype repair failed: {e}", file=sys.stderr)

def repair_container_xml(temp_dir):
    container_path = os.path.join(temp_dir, 'META-INF', 'container.xml')
    os.makedirs(os.path.dirname(container_path), exist_ok=True)
    try:
        opf_files = find_opf(temp_dir)
        if not opf_files: return
        if not os.path.exists(container_path):
            opf_rel = os.path.relpath(opf_files[0], temp_dir).replace(os.sep, '/')
            write_text(container_path,
                       f'<?xml version="1.0" encoding="UTF-8"?>\n'
                       f'<container version="1.0"'
                       f' xmlns="urn:oasis:names:tc:opendocument:xmlns:container">\n'
                       f'  <rootfiles>\n'
                       f'    <rootfile full-path="{opf_rel}"'
                       f' media-type="application/oebps-package+xml"/>\n'
                       f'  </rootfiles>\n</container>\n')
            return
        content = read_text(container_path)
        fp_m    = re.search(r'full-path="([^"]+)"', content, re.IGNORECASE)
        if fp_m:
            declared = os.path.normpath(os.path.join(temp_dir, fp_m.group(1)))
            if not os.path.exists(declared):
                opf_rel = os.path.relpath(opf_files[0], temp_dir).replace(os.sep, '/')
                content = re.sub(
                    r'full-path="[^"]+"', f'full-path="{opf_rel}"',
                    content, flags=re.IGNORECASE)
                write_text(container_path, content)
    except Exception as e: print(f"\n  Warning: container.xml repair failed: {e}", file=sys.stderr)

def repair_manifest_media_types(temp_dir):
    EXT_TO_MEDIA = {
        '.html': 'application/xhtml+xml', '.xhtml': 'application/xhtml+xml',
        '.htm': 'application/xhtml+xml', '.css': 'text/css',
        '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png',
        '.gif': 'image/gif', '.svg': 'image/svg+xml', '.webp': 'image/webp',
        '.ttf': 'font/ttf', '.otf': 'font/otf', '.woff': 'font/woff', '.woff2': 'font/woff2',
        '.js': 'application/javascript', '.mp3': 'audio/mpeg', '.mp4': 'video/mp4',
        '.ncx': 'application/x-dtbncx+xml', '.opf': 'application/oebps-package+xml',
    }
    for opf_path in find_opf(temp_dir):
        try:
            content = read_text(opf_path)
            def fix_media_type(match):
                tag    = match.group(0)
                href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                if not href_m: return tag
                ext      = os.path.splitext(href_m.group(1))[1].lower()
                expected = EXT_TO_MEDIA.get(ext)
                if not expected: return tag   # unknown extension — leave alone
                mt_m = re.search(r'\bmedia-type="([^"]+)"', tag, re.IGNORECASE)
                if mt_m:
                    # Wrong media-type — correct it
                    if mt_m.group(1).lower() != expected:
                        return tag.replace(mt_m.group(0), f'media-type="{expected}"')
                    return tag
                else:
                    # Missing media-type entirely — inject before closing />
                    return re.sub(r'(\s*/\s*>|\s*>)$',
                                  f' media-type="{expected}"\\1',
                                  tag.rstrip(), count=1)
            new_content = re.sub(r'<item\s[^>]*/\s*>', fix_media_type, content, flags=re.IGNORECASE)
            new_content = re.sub(
                r'<item\s[^>]*>(?:</item>)?', fix_media_type,
                new_content, flags=re.IGNORECASE)
            write_if_changed(opf_path, content, new_content)
        except Exception as e:
            _warn("media-type repair failed", opf_path, e)

def repair_duplicate_manifest_ids(temp_dir):
    for opf_path in find_opf(temp_dir):
        try:
            content  = read_text(opf_path)
            seen_ids = {}
            renames  = {}
            for item_id in re.findall(r'<item\s[^>]*\bid="([^"]+)"', content, re.IGNORECASE):
                seen_ids[item_id] = seen_ids.get(item_id, 0) + 1
            changed, counter = False, {}
            def dedup_id(match):
                nonlocal changed
                tag     = match.group(0)
                id_m    = re.search(r'\bid="([^"]+)"', tag, re.IGNORECASE)
                if not id_m: return tag
                item_id = id_m.group(1)
                if seen_ids.get(item_id, 1) <= 1: return tag
                counter[item_id] = counter.get(item_id, 0) + 1
                if counter[item_id] == 1: return tag
                new_id = f'{item_id}-dup{counter[item_id]}'
                renames[item_id] = new_id; changed = True
                return tag.replace(f'id="{item_id}"', f'id="{new_id}"')
            content = re.sub(r'<item\s[^>]*/\s*>', dedup_id, content, flags=re.IGNORECASE)
            content = re.sub(r'<item\s[^>]*>(?:</item>)?', dedup_id, content, flags=re.IGNORECASE)
            for old_id, new_id in renames.items():
                content = re.sub(
                    rf'(<itemref\s[^>]*\bidref="){re.escape(old_id)}"',
                    rf'\1{new_id}"', content, flags=re.IGNORECASE)
            if changed: write_text(opf_path, content)
        except Exception as e:
            _warn("manifest ID dedup failed", opf_path, e)


def repair_duplicate_manifest_hrefs(temp_dir):
    # Feature B: Detect and remove duplicate href= values in the OPF manifest.
    #
    # If two <item> elements point to the same file, one is redundant.  We keep
    # the first occurrence (in document order) and remove the duplicate item tag
    # and any corresponding <itemref> in the spine.
    #
    # Duplicate hrefs most commonly arise from authoring tools that add the same
    # image or stylesheet twice under different manifest IDs.
    for opf_path in find_opf(temp_dir):
        try:
            content = read_text(opf_path)
            seen_hrefs: dict[str, str] = {}  # normalised_href → first_item_id
            dup_ids: set[str] = set()

            for m in re.finditer(r'<item\s[^>]*/\s*>', content, re.IGNORECASE):
                tag    = m.group(0)
                id_m   = re.search(r'\bid="([^"]+)"',   tag, re.IGNORECASE)
                href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                if not id_m or not href_m: continue
                item_id = id_m.group(1)
                href    = href_m.group(1).lower().rstrip('/')  # normalise case
                if href in seen_hrefs:
                    dup_ids.add(item_id)
                    print(f"\n  Warning: duplicate manifest href removed: "
                          f"id='{item_id}' href='{href_m.group(1)}'", file=sys.stderr)
                else: seen_hrefs[href] = item_id

            if not dup_ids: continue

            def remove_dup_item(m):
                tag   = m.group(0)
                id_m2 = re.search(r'\bid="([^"]+)"', tag, re.IGNORECASE)
                if id_m2 and id_m2.group(1) in dup_ids: return ''
                return tag

            content = re.sub(r'<item\s[^>]*/\s*>', remove_dup_item, content, flags=re.IGNORECASE)
            # Also remove spine itemrefs for the removed items
            for dup_id in dup_ids:
                content = re.sub(
                    rf'<itemref\s[^>]*\bidref="{re.escape(dup_id)}"[^>]*/?>',
                    '', content, flags=re.IGNORECASE)
            write_text(opf_path, content)
        except Exception as e:
            _warn("duplicate href repair failed", opf_path, e)


def validate_unique_identifier(temp_dir):
    # Feature B / Feature J: Ensure the package unique-identifier attribute on
    # <package> references an existing <dc:identifier id="..."> element.
    #
    # If the reference is broken, reset it to "BookId" and ensure a dc:identifier
    # with that id exists (creating a UUID one if necessary).
    for opf_path in find_opf(temp_dir):
        try:
            content, changed = read_text(opf_path), False
            pkg_m  = re.search(r'<package\b[^>]*\bunique-identifier="([^"]+)"',
                                content, re.IGNORECASE)
            if not pkg_m:
                # No unique-identifier attribute at all — inject it pointing to BookId
                content = re.sub(
                    r'<package\b',
                    '<package unique-identifier="BookId"',
                    content, count=1, flags=re.IGNORECASE)
                changed = True
                uid_val = 'BookId'
            else: uid_val = pkg_m.group(1)

            # Check that an element with id=uid_val exists in <metadata>
            if not re.search(rf'\bid="{re.escape(uid_val)}"', content, re.IGNORECASE):
                # Broken reference — add a dc:identifier with id=BookId
                new_uuid = f'urn:uuid:{uuid.uuid4()}'
                content = re.sub(
                    r'(<metadata\b[^>]*>)',
                    rf'\1\n  <dc:identifier id="BookId">{new_uuid}</dc:identifier>',
                    content, count=1, flags=re.IGNORECASE)
                # Also fix the unique-identifier attribute to point to BookId
                content = re.sub(
                    r'(unique-identifier=")[^"]*(")',
                    r'\1BookId\2', content, flags=re.IGNORECASE)
                print(f"\n  Warning: unique-identifier was broken; injected "
                      f"dc:identifier id='BookId' with UUID.", file=sys.stderr)
                changed = True

            if changed: write_text(opf_path, content)
        except Exception as e:
            print(f"\n  Warning: unique-identifier validation failed for {opf_path}: {e}",
                  file=sys.stderr)

def repair_opf_metadata(temp_dir):
    for opf_path in find_opf(temp_dir):
        try:
            content, opf_dir, changed = read_text(opf_path), os.path.dirname(opf_path), False
            if not re.search(r'<metadata', content, re.IGNORECASE):
                content = re.sub(r'(<package[^>]*>)',
                                 r'\1\n<metadata'
                                 r' xmlns:dc="http://purl.org/dc/elements/1.1/">\n</metadata>',
                                 content, flags=re.IGNORECASE)
                changed = True
            if not re.search(r'<dc:identifier', content, re.IGNORECASE):
                uid = f'urn:uuid:{uuid.uuid4()}'
                content = re.sub(r'(<metadata[^>]*>)',
                                 rf'\1\n  <dc:identifier id="uid">{uid}</dc:identifier>',
                                 content, flags=re.IGNORECASE)
                changed = True
            if not re.search(r'<dc:title', content, re.IGNORECASE):
                title = os.path.splitext(os.path.basename(opf_path))[0]
                content = re.sub(r'(<metadata[^>]*>)',
                                 rf'\1\n  <dc:title>{title}</dc:title>',
                                 content, flags=re.IGNORECASE)
                changed = True
            if not re.search(r'<dc:language', content, re.IGNORECASE):
                lang = _get_book_language(os.path.dirname(opf_dir)) or 'en'
                content = re.sub(r'(<metadata[^>]*>)',
                                 rf'\1\n  <dc:language>{lang}</dc:language>',
                                 content, flags=re.IGNORECASE)
                changed = True
            # Inject dc:creator="Unknown" when no author element exists
            if not re.search(r'<dc:creator', content, re.IGNORECASE):
                content = re.sub(r'(<metadata[^>]*>)',
                                 r'\1\n  <dc:creator opf:role="aut">Unknown</dc:creator>',
                                 content, flags=re.IGNORECASE)
                changed = True
            # Normalise year-only dc:date values to ISO 8601 (e.g. "2020" → "2020-01-01")
            def _fix_year_date(m):
                nonlocal changed
                tag, val = m.group(0), m.group(1).strip()
                if re.match(r'^\d{4}$', val):
                    changed = True
                    return tag.replace(val, val + '-01-01')
                return tag
            content = re.sub(r'<dc:date[^>]*>([^<]+)</dc:date>',
                             _fix_year_date, content, flags=re.IGNORECASE)
            has_nav = bool(re.search(
                r'<item\s[^>]*\bproperties="[^"]*\bnav\b[^"]*"', content, re.IGNORECASE))
            expected_ver = '3.0' if has_nav else '2.0'
            ver_m = re.search(r'<package[^>]*\bversion="([^"]+)"', content, re.IGNORECASE)
            if not ver_m:
                content = re.sub(r'<package\b', f'<package version="{expected_ver}"',
                                 content, count=1, flags=re.IGNORECASE)
                changed = True
            elif ver_m.group(1) not in ('2.0', '3.0'):
                content = content.replace(ver_m.group(0),
                                          ver_m.group(0).replace(ver_m.group(1), expected_ver))
                changed = True
            if changed: write_text(opf_path, content)
        except Exception as e:
            _warn("OPF metadata repair failed", opf_path, e)

def repair_ncx_play_order(temp_dir):
    for ncx_path in find_ncx(temp_dir):
        try:
            content, counter = read_text(ncx_path), [0]
            def renumber(match):
                counter[0] += 1
                tag = match.group(0)
                if re.search(r'\bplayOrder\s*=', tag, re.IGNORECASE):
                    return re.sub(
                        r'\bplayOrder="[^"]*"',
                        f'playOrder="{counter[0]}"', tag, flags=re.IGNORECASE)
                return re.sub(r'(<navPoint\b)', rf'\1 playOrder="{counter[0]}"', tag)
            new_content = re.sub(r'<navPoint\b[^>]*>', renumber, content, flags=re.IGNORECASE)
            if new_content != content: write_text(ncx_path, new_content)
        except Exception as e:
            _warn("NCX playOrder repair failed", ncx_path, e)

def repair_nav_in_spine(temp_dir):
    for opf_path in find_opf(temp_dir):
        try:
            content = read_text(opf_path)
            nav_id_m = re.search(
                r'<item\s[^>]*\bproperties="[^"]*\bnav\b[^"]*"[^>]*\bid="([^"]+)"',
                content, re.IGNORECASE)
            if not nav_id_m:
                nav_id_m = re.search(
                    r'<item\s[^>]*\bid="([^"]+)"[^>]*\bproperties="[^"]*\bnav\b[^"]*"',
                    content, re.IGNORECASE)
            if not nav_id_m: continue
            nav_id = nav_id_m.group(1)
            if re.search(rf'<itemref\s[^>]*\bidref="{re.escape(nav_id)}"', content, re.IGNORECASE):
                continue
            nav_itemref = f'<itemref idref="{nav_id}" linear="no"/>'
            content = re.sub(r'(<spine\b[^>]*>)', rf'\1\n    {nav_itemref}',
                             content, count=1, flags=re.IGNORECASE)
            write_text(opf_path, content)
        except Exception as e:
            print(f"\n  Warning: NAV spine registration failed for "
                  f"{opf_path}: {e}", file=sys.stderr)

def repair_spine_linear_attributes(temp_dir):
    for opf_path in find_opf(temp_dir):
        try:
            content, opf_dir, changed = read_text(opf_path), os.path.dirname(opf_path), False
            non_linear_ids = set()
            nav_m = re.search(
                r'<item\s[^>]*\bproperties="[^"]*\bnav\b[^"]*"[^>]*\bid="([^"]+)"',
                content, re.IGNORECASE)
            if nav_m: non_linear_ids.add(nav_m.group(1))
            cover_href = _get_cover_spine_href(content, opf_dir)
            if cover_href:
                cover_id_m = re.search(
                    rf'<item\s[^>]*\bhref="{re.escape(cover_href)}"[^>]*\bid="([^"]+)"',
                    content, re.IGNORECASE)
                if cover_id_m: non_linear_ids.add(cover_id_m.group(1))
            def fix_linear(match):
                nonlocal changed
                tag     = match.group(0)
                idref_m = re.search(r'\bidref="([^"]+)"', tag, re.IGNORECASE)
                if not idref_m: return tag
                idref    = idref_m.group(1)
                linear_m = re.search(r'\blinear="([^"]+)"', tag, re.IGNORECASE)
                if idref in non_linear_ids:
                    if not linear_m:
                        changed = True
                        return re.sub(r'(<itemref\b)', r'\1 linear="no"', tag)
                    elif linear_m.group(1).lower() != 'no':
                        changed = True
                        return tag.replace(linear_m.group(0), 'linear="no"')
                return tag
            content = re.sub(r'<itemref\s[^>]*/>', fix_linear, content, flags=re.IGNORECASE)
            if changed: write_text(opf_path, content)
            # If ALL spine items ended up linear="no", the book appears blank on most readers.
            # Upgrade the first non-cover/non-nav itemref to linear="yes" as a fix.
            all_idrefs = re.findall(r'<itemref\s[^>]*\/>', content, re.IGNORECASE)
            linear_no  = [t for t in all_idrefs if re.search(r'linear="no"', t, re.IGNORECASE)]
            if all_idrefs and len(linear_no) == len(all_idrefs):
                # Find first itemref that is NOT the nav or cover
                def _promote_first(m):
                    tag     = m.group(0)
                    idref_m = re.search(r'\bidref="([^"]+)"', tag, re.IGNORECASE)
                    if idref_m and idref_m.group(1) not in non_linear_ids:
                        return tag.replace('linear="no"', 'linear="yes"')
                    return tag
                fixed = re.sub(r'<itemref\s[^>]*/>', _promote_first, content,
                               count=1, flags=re.IGNORECASE)
                if fixed != content:
                    write_text(opf_path, fixed)
                    print(f"  Warning: all spine items were linear=no; promoted first content item",
                          file=sys.stderr)
        except Exception as e:
            _warn("spine linear attribute repair failed", exc=e)

def repair_absolute_hrefs(temp_dir):
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            content, changed = read_text(opf_path), False
            def make_relative(match):
                nonlocal changed
                tag    = match.group(0)
                href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                if not href_m: return tag
                href = href_m.group(1)
                if not href.startswith('/'): return tag
                abs_candidate = os.path.normpath(os.path.join(temp_dir, href.lstrip('/')))
                if os.path.exists(abs_candidate):
                    rel = os.path.relpath(abs_candidate, opf_dir).replace(os.sep, '/')
                    changed = True
                    return tag.replace(href_m.group(0), f'href="{rel}"')
                return tag
            content = re.sub(r'<item\s[^>]*/>', make_relative, content, flags=re.IGNORECASE)
            if changed: write_text(opf_path, content)
        except Exception as e:
            _warn("absolute href repair failed", opf_path, e)


def normalize_href_case(temp_dir):
    # Feature C: Case-sensitivity href normalization.
    #
    # On Windows and macOS the filesystem is case-insensitive, so broken case
    # references are invisible during authoring but fail on Linux-based reading
    # systems (Kobo firmware, Adobe RMSDK on some platforms) and iOS (case-sensitive
    # APFS volumes).
    #
    # Strategy (safe, no file renames):
    # • Build a map from lower-case filename → actual filename on disk.
    # • Scan OPF manifest href= and HTML href=/src= attributes.
    # • If a reference resolves to a real file after case-folding but NOT by
    # exact case match, rewrite the reference to the on-disk case.
    # • Never renames files — only fixes references.
    # Build case map: lower_abs_path → actual_abs_path
    case_map: dict[str, str] = {}
    for root, dirs, files in os.walk(temp_dir):
        for f in files:
            abs_path  = os.path.join(root, f)
            lower_key = abs_path.lower()
            if lower_key not in case_map: case_map[lower_key] = abs_path
            # If there's a collision (genuine case conflict) leave both — we can't resolve
        for d in dirs:
            abs_d     = os.path.join(root, d)
            lower_key = abs_d.lower()
            if lower_key not in case_map: case_map[lower_key] = abs_d

    def _fix_href_case(base_dir, href):
        # Return corrected href if case mismatch found, else None.
        if href.startswith(('http://', 'https://', 'mailto:', 'data:', '#')): return None
        path_part = href.split('#')[0]
        if not path_part: return None
        candidate = os.path.normpath(os.path.join(base_dir, path_part))
        if os.path.exists(candidate): return None  # exact match — no case issue
        actual = case_map.get(candidate.lower())
        if actual and os.path.exists(actual):
            correct_rel = os.path.relpath(actual, base_dir).replace(os.sep, '/')
            if '#' in href: correct_rel += '#' + href.split('#', 1)[1]
            return correct_rel
        return None

    # Fix OPF manifest hrefs
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            content, changed = read_text(opf_path), False
            def fix_opf_case(m):
                nonlocal changed
                tag    = m.group(0)
                href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                if not href_m: return tag
                corrected = _fix_href_case(opf_dir, href_m.group(1))
                if corrected:
                    changed = True
                    return tag.replace(href_m.group(0), f'href="{corrected}"')
                return tag
            content = re.sub(r'<item\s[^>]*/\s*>', fix_opf_case, content, flags=re.IGNORECASE)
            if changed: write_text(opf_path, content)
        except Exception as e:
            print(f"\n  Warning: OPF case normalization failed for "
                  f"{opf_path}: {e}", file=sys.stderr)

    # Fix HTML/XHTML href= and src= references
    for html_path in find_html(temp_dir):
        html_dir = os.path.dirname(html_path)
        try:
            content, changed = read_text(html_path), False
            def fix_html_case(m):
                nonlocal changed
                attr   = m.group(1)
                href   = m.group(2)
                corrected = _fix_href_case(html_dir, href)
                if corrected:
                    changed = True
                    return f'{attr}="{corrected}"'
                return m.group(0)
            content = re.sub(r'\b(href|src)="([^"]*)"', fix_html_case,
                             content, flags=re.IGNORECASE)
            if changed: write_text(html_path, content)
        except Exception as e:
            print(f"\n  Warning: HTML case normalization failed for "
                  f"{html_path}: {e}", file=sys.stderr)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 13b — DUPLICATE ID REPAIR
# ═══════════════════════════════════════════════════════════════════════════════

def _rename_map_for_doc(html_text):
    # Scan html_text for all id="..." attributes.  For any id that appears more
    # than once, build a deterministic rename map:
    # second occurrence  → original_id-dup-1
    # third occurrence   → original_id-dup-2
    # …
    #
    # Skips ids that already match the -dup-N suffix pattern to avoid
    # double-suffixing on repeated runs.
    #
    # Returns:
    # renames : list of (old_id, new_id) in document order
    # (only duplicate occurrences — first occurrence keeps its id)
    # seen    : set of all ids found (including unique ones)
    DUP_PAT = re.compile(r'-dup-\d+$')
    id_pat   = re.compile(r'\bid="([^"]+)"', re.IGNORECASE)

    counts   = {}   # id → how many times seen so far while scanning
    renames  = []   # [(old_id, new_id), ...]

    for m in id_pat.finditer(html_text):
        raw_id = m.group(1)
        # Never touch ids that are already our own -dup-N suffixed form
        if DUP_PAT.search(raw_id):
            counts.setdefault(raw_id, 0)
            counts[raw_id] += 1
            continue
        counts.setdefault(raw_id, 0)
        counts[raw_id] += 1
        if counts[raw_id] > 1:
            new_id = f'{raw_id}-dup-{counts[raw_id] - 1}'
            renames.append((raw_id, new_id))

    return renames, set(counts.keys())


def _apply_id_renames_in_doc(html_text, renames):
    # Apply a rename list to a single HTML document:
    # • Rename the duplicate id= attributes themselves (in document order,
    # skipping the first occurrence which is the keeper).
    #
    # Same-document href="#id" links are intentionally NOT rewritten here.
    # They should continue pointing to the original (keeper) id, which is
    # unchanged. Cross-document links that referenced the original file+fragment
    # are updated by the Phase 3 pass in repair_duplicate_ids. (Bug #4 fix)
    #
    # renames is a list of (old_id, new_id) pairs in document order.
    if not renames: return html_text

    id_pat = re.compile(r'\bid="([^"]+)"', re.IGNORECASE)

    id_queue = defaultdict(list)
    for old_id, new_id in renames: id_queue[old_id].append(new_id)

    id_seen = defaultdict(int)
    result   = []
    prev_end = 0
    for m in id_pat.finditer(html_text):
        raw_id = m.group(1)
        id_seen[raw_id] += 1
        result.append(html_text[prev_end:m.start()])
        if id_seen[raw_id] > 1 and id_queue[raw_id]:
            new_id = id_queue[raw_id].pop(0)
            result.append(f'id="{new_id}"')
        else: result.append(m.group(0))
        prev_end = m.end()
    result.append(html_text[prev_end:])
    return ''.join(result)


def repair_duplicate_ids(temp_dir):
    # Per-document duplicate ID repair:
    #
    # 1. Scan each XHTML file for duplicate id= attributes.
    # 2. Rename duplicates deterministically: original-dup-1, original-dup-2 …
    # (already-suffixed ids are skipped to prevent double-processing).
    # 3. Update same-document href="#id" references.
    # 4. Update cross-document links in all other XHTML files that referenced
    # the original file+fragment.
    #
    # This runs before structural auto-repair so the repaired ids are clean when
    # validate_structure inspects the files.
    html_files = find_html(temp_dir)
    if not html_files: return

    # Map from abs_path → relative arc name (needed for cross-doc link matching)
    nav_paths = set(find_nav(temp_dir))

    # Phase 1: collect renames per file  {abs_path: [(old_id, new_id), ...]}
    file_renames = {}
    for html_path in html_files:
        try:
            html    = read_text(html_path)
            renames, _ = _rename_map_for_doc(html)
            if renames: file_renames[html_path] = renames
        except Exception as e:
            print(f"\n  Warning: duplicate-ID scan failed for {html_path}: {e}",
                  file=sys.stderr)

    if not file_renames: return

    # Phase 2: apply renames within each document
    for html_path, renames in file_renames.items():
        try:
            html = read_text(html_path)
            html = _apply_id_renames_in_doc(html, renames)
            write_text(html_path, html)
        except Exception as e:
            print(f"\n  Warning: duplicate-ID rename failed for {html_path}: {e}",
                  file=sys.stderr)

    # Phase 3: fix cross-document links in *all* HTML files
    # For each file that had renames, other files may have href="that-file#old-id"
    for html_path, renames in file_renames.items():
        # Build the set of basename + relative path fragments to match
        # We match on basename (most common) and full path segment where possible
        target_basename = os.path.basename(html_path)

        for other_path in html_files:
            if other_path == html_path: continue
            try:
                other = read_text(other_path)
                changed = False
                for old_id, new_id in renames:
                    # Match href="[anything/]filename.xhtml#old_id"
                    # Use a pattern that catches both relative and bare refs
                    pat = re.compile(
                        r'(<a\b[^>]*\bhref="[^"]*' + re.escape(target_basename)
                        + r'#)' + re.escape(old_id) + r'("',
                        re.IGNORECASE)
                    new_other, n = pat.subn(r'\1' + new_id + r'\2', other)
                    if n:
                        other   = new_other
                        changed = True
                if changed: write_text(other_path, other)
            except Exception as e:
                print(f"\n  Warning: cross-doc ID link update failed for "
                      f"{other_path}: {e}", file=sys.stderr)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 13c — FRAGMENT LINK VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def _build_fragment_index(html_files):
    # Scan every HTML file and record all id= values defined in it.
    # Returns: {abs_path: frozenset(id_strings)}
    index = {}
    id_pat = re.compile(r'\bid="([^"]+)"', re.IGNORECASE)
    for html_path in html_files:
        try:
            text = read_text(html_path)
            index[html_path] = frozenset(m.group(1) for m in id_pat.finditer(text))
        except Exception: index[html_path] = frozenset()
    return index


def validate_fragment_links(temp_dir):
    # For every XHTML/HTML file, inspect every href= and src= attribute:
    #
    # Case 1 — href points to a file that does not exist in the EPUB:
    # Strip the entire href attribute, keep the element text.
    # (e.g. <a href="missing.xhtml">text</a> → <a>text</a>)
    #
    # Case 2 — href points to file#fragment where the file exists but the
    # fragment id does not exist in that document:
    # Strip only the fragment, keep the file reference.
    # (e.g. href="ch1.xhtml#gone" → href="ch1.xhtml")
    #
    # Case 3 — same-document href="#fragment" where the fragment does not
    # exist in this document:
    # Strip the entire href attribute, keep anchor text.
    # (e.g. <a href="#gone">text</a> → <a>text</a>)
    #
    # Never modifies:
    # • Valid links (file exists, fragment exists or absent).
    # • External links (http:// or https://).
    # • NAV documents (their links are structural, not navigational).
    # • Links inside <nav> elements (same reason).
    #
    # Broken src= (images, CSS) are left for cleanup_manifest to handle —
    # they are not hyperlinks so removing href makes no sense.
    html_files = find_html(temp_dir)
    if not html_files: return

    nav_paths      = set(find_nav(temp_dir))
    fragment_index = _build_fragment_index(html_files)

    # href= pattern: captures everything inside href="..."
    href_pat = re.compile(r'\bhref="([^"]*)"', re.IGNORECASE)

    for html_path in html_files:
        # Skip NAV documents — their TOC links are structural
        if html_path in nav_paths: continue

        html_dir = os.path.dirname(html_path)
        try:
            original = read_text(html_path)
        except Exception as e:
            print(f"\n  Warning: fragment validation read failed for {html_path}: {e}",
                  file=sys.stderr)
            continue

        changed = False

        def fix_href(m):
            nonlocal changed
            raw = m.group(1)

            # Leave external links alone
            if raw.startswith(('http://', 'https://', 'mailto:', 'tel:')): return m.group(0)
            # Leave empty hrefs alone
            if not raw: return m.group(0)

            # Split into path part and fragment
            if '#' in raw: path_part, fragment = raw.split('#', 1)
            else: path_part, fragment = raw, ''

            # ── Same-document fragment link (#id) ────────────────────────────
            if not path_part:
                if not fragment: return m.group(0)
                # Check if this fragment exists in the current document
                if fragment in fragment_index.get(html_path, frozenset()):
                    return m.group(0)   # valid
                # Dead same-doc fragment — strip the whole href
                changed = True
                return ''   # removes href="..." entirely

            # ── Cross-document link ──────────────────────────────────────────
            # Resolve path_part relative to this document's directory
            # Handle both relative paths and paths with ../ segments
            try:
                target_abs = os.path.normpath(
                    os.path.join(html_dir, path_part))
            except Exception: return m.group(0)

            if not os.path.exists(target_abs):
                # Whole file is missing — strip href, keep anchor text
                changed = True
                return ''

            # File exists — now check fragment if present
            if fragment:
                target_ids = fragment_index.get(target_abs, None)
                if target_ids is None:
                    # Not an HTML file we indexed (e.g. PDF link) — leave alone
                    return m.group(0)
                if fragment not in target_ids:
                    # Fragment is dead — strip fragment, keep file reference
                    changed = True
                    return f'href="{path_part}"'

            return m.group(0)   # valid, no change

        new_html = href_pat.sub(fix_href, original)

        if changed:
            try:
                write_text(html_path, new_html)
            except Exception as e:
                print(f"\n  Warning: fragment validation write failed for {html_path}: {e}",
                      file=sys.stderr)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 13d — RELATIVE PATH NORMALISATION
# ═══════════════════════════════════════════════════════════════════════════════

def _resolve_relative_href(base_dir, href, temp_dir):
    # Given a raw href string (relative path, possibly with wrong depth),
    # try to resolve it to an absolute path that actually exists on disk.
    #
    # Returns the corrected relative path (relative to base_dir) if the file
    # can be found, or None if no resolution is possible.
    #
    # Strategy:
    # 1. Resolve naively relative to base_dir → if it exists, it's already correct.
    # 2. Try resolving relative to temp_dir root (handles leading-slash paths
    # that should be ZIP-root-relative).
    # 3. Search the entire temp_dir for a file with the same basename → if
    # exactly one match, rewrite to point there.
    # (Handles wrong-depth ../ paths like ../../Images/cover.jpg)
    #
    # Never guesses when there are multiple files with the same basename.
    # Strip fragment and query for filesystem resolution
    clean_href = href.split('#')[0].split('?')[0]
    if not clean_href: return None

    # Strategy 1: naive relative resolve
    candidate = os.path.normpath(os.path.join(base_dir, clean_href))
    if os.path.exists(candidate): return None  # Already correct — no rewrite needed

    # Strategy 2: temp_dir-relative (for paths that start with /)
    if href.startswith('/'):
        candidate2 = os.path.normpath(os.path.join(temp_dir, clean_href.lstrip('/')))
        if os.path.exists(candidate2):
            rel = os.path.relpath(candidate2, base_dir).replace(os.sep, '/')
            return rel

    # Strategy 3: basename search using pre-built index (O(1) lookup vs O(n) walk)
    target_basename = os.path.basename(clean_href)
    if not target_basename: return None
    basename_lo = target_basename.lower()
    idx = _epub_index.get(temp_dir, {})
    basename_map = idx.get("basename_map", {})
    if basename_map:
        candidates = basename_map.get(basename_lo, [])
        # Filter out META-INF
        matches = [p for p in candidates if 'META-INF' not in p]
    else:
        # Fallback: walk (should rarely occur — index should always be built)
        matches = []
        for root, _, files in os.walk(temp_dir):
            if 'META-INF' in root: continue
            for f in files:
                if f.lower() == basename_lo: matches.append(os.path.join(root, f))
    if len(matches) == 1:
        rel = os.path.relpath(matches[0], base_dir).replace(os.sep, '/')
        # Preserve fragment if original had one
        if '#' in href: rel += '#' + href.split('#', 1)[1]
        return rel

    return None  # ambiguous or not found


def repair_relative_paths(temp_dir):
    # Repair broken relative href/src paths in:
    # (a) OPF manifest <item href="..."> elements
    # (b) HTML/XHTML href= and src= attributes
    #
    # Approach:
    # For each path that cannot be resolved naively, try _resolve_relative_href.
    # If a unique match is found, rewrite the path.
    # If no unique match, leave the broken reference for cleanup_manifest/
    # validate_fragment_links to handle.
    #
    # Never rewrites paths that already resolve correctly.
    # Never guesses when basename is ambiguous.
    # Operates after duplicate-ID repair and before manifest cleanup so
    # cleanup_manifest can then remove any truly unresolvable references.
    # ── (a) OPF manifest items ────────────────────────────────────────────────
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            content, changed = read_text(opf_path), False

            def fix_opf_href(m):
                nonlocal changed
                tag    = m.group(0)
                href_m = re.search(r'\bhref="([^"]+)"', tag, re.IGNORECASE)
                if not href_m: return tag
                href = href_m.group(1)
                # Skip already-correct paths and external URLs
                if href.startswith(('http://', 'https://')): return tag
                new_rel = _resolve_relative_href(opf_dir, href, temp_dir)
                if new_rel and new_rel != href:
                    changed = True
                    return tag.replace(href_m.group(0), f'href="{new_rel}"')
                return tag

            content = re.sub(r'<item\s[^>]*/\s*>', fix_opf_href, content, flags=re.IGNORECASE)
            if changed: write_text(opf_path, content)
        except Exception as e:
            print(f"\n  Warning: OPF path repair failed for {opf_path}: {e}",
                  file=sys.stderr)

    # ── (b) HTML/XHTML href= and src= ────────────────────────────────────────
    nav_paths = set(find_nav(temp_dir))
    for html_path in find_html(temp_dir):
        html_dir = os.path.dirname(html_path)
        try:
            content, changed = read_text(html_path), False

            def fix_html_ref(m):
                nonlocal changed
                attr_name = m.group(1)   # "href" or "src"
                raw_href  = m.group(2)

                # Skip external links, data URIs, anchors, empty
                if (not raw_href
                        or raw_href.startswith(('#', 'http://', 'https://',
                                                'mailto:', 'tel:', 'data:'))):
                    return m.group(0)

                new_rel = _resolve_relative_href(html_dir, raw_href, temp_dir)
                if new_rel and new_rel != raw_href:
                    changed = True
                    return f'{attr_name}="{new_rel}"'
                return m.group(0)

            # Match href="..." and src="..." (both are path-bearing attributes)
            content = re.sub(r'\b(href|src)="([^"]*)"', fix_html_ref,
                             content, flags=re.IGNORECASE)
            if changed: write_text(html_path, content)
        except Exception as e:
            print(f"\n  Warning: HTML path repair failed for {html_path}: {e}",
                  file=sys.stderr)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 13e — CIRCULAR / INVALID SPINE REFERENCE DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def detect_circular_spine(temp_dir):
    # Detect and remove invalid spine itemrefs:
    #
    # Case 1 — Non-content item in spine:
    # A spine itemref that resolves to a non-HTML file (image, CSS, font,
    # NCX, OPF, etc.) is structurally invalid. Remove the itemref.
    # Exception: the NAV document is allowed in spine (linear="no").
    #
    # Case 2 — Self-referencing itemref:
    # An itemref whose idref resolves to the OPF file itself. Remove it.
    #
    # Case 3 — Duplicate idrefs:
    # Already handled by remove_duplicate_spine_items, but we guard here
    # too for safety.
    #
    # Does NOT attempt to detect content-level loops (page A links to page B
    # which links back to page A) — those are valid reading structures, not
    # EPUB structural errors.
    CONTENT_EXTENSIONS = {'.html', '.xhtml', '.htm'}

    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            content = read_text(opf_path)
            id_to_href = _parse_manifest_items(content)

            # Build set of ids that are valid spine content
            # (HTML/XHTML files, including the NAV document)
            valid_spine_ids = set()
            invalid_reasons = {}   # id → reason string for logging
            for item_id, href in id_to_href.items():
                ext = os.path.splitext(href.split('#')[0])[1].lower()
                if ext in CONTENT_EXTENSIONS: valid_spine_ids.add(item_id)
                else:
                    invalid_reasons[item_id] = (
                        f"non-content file ({ext or 'no extension'}): {href}")

            # Also flag self-references (itemref pointing at the OPF itself)
            opf_basename = os.path.basename(opf_path)
            for item_id, href in id_to_href.items():
                if os.path.basename(href) == opf_basename:
                    invalid_reasons[item_id] = f"references the OPF itself: {href}"
                    valid_spine_ids.discard(item_id)

            changed = False

            def remove_invalid_itemref(match):
                nonlocal changed
                tag     = match.group(0)
                idref_m = re.search(r'\bidref="([^"]+)"', tag, re.IGNORECASE)
                if not idref_m: return tag
                idref = idref_m.group(1)
                if idref in invalid_reasons:
                    print(f"\n  Warning: removing invalid spine itemref idref='{idref}': "
                          f"{invalid_reasons[idref]}", file=sys.stderr)
                    changed = True
                    return ''
                return tag

            new_content = re.sub(r'<itemref\s[^>]*/>',
                                  remove_invalid_itemref, content, flags=re.IGNORECASE)
            if changed: write_text(opf_path, new_content)

        except Exception as e:
            print(f"\n  Warning: circular spine detection failed for {opf_path}: {e}",
                  file=sys.stderr)

def repair_ncx_uid_sync(temp_dir):
    # Sync NCX dtb:uid content to match the OPF unique-identifier value.
    # A mismatch causes EPUB validation failures and can confuse reading systems
    # that rely on the uid for library de-duplication.
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            opf = read_text(opf_path)
            # Find OPF unique-identifier attribute value
            uid_id_m = re.search(r'<package[^>]*\bunique-identifier="([^"]+)"', opf, re.IGNORECASE)
            if not uid_id_m: continue
            uid_id  = uid_id_m.group(1)
            # Find the dc:identifier with that id
            uid_val_m = re.search(
                rf'<dc:identifier[^>]*\bid="{re.escape(uid_id)}"[^>]*>([^<]+)</dc:identifier>',
                opf, re.IGNORECASE)
            if not uid_val_m: continue
            uid_val = uid_val_m.group(1).strip()
            if not uid_val: continue
            # Find the NCX file
            ncx_m = re.search(
                r'<item\s[^>]*\bmedia-type="application/x-dtbncx\+xml"[^>]*\bhref="([^"]+)"',
                opf, re.IGNORECASE)
            if not ncx_m: continue
            ncx_path = os.path.normpath(os.path.join(opf_dir, ncx_m.group(1)))
            if not os.path.exists(ncx_path): continue
            ncx = read_text(ncx_path)
            # Check current dtb:uid value
            dtb_m = re.search(
                r'(<meta\s[^>]*\bname="dtb:uid"[^>]*\bcontent=")([^"]*)("/>|">)',
                ncx, re.IGNORECASE)
            if dtb_m:
                if dtb_m.group(2).strip() == uid_val: continue  # already in sync
                new_ncx = ncx[:dtb_m.start(2)] + uid_val + ncx[dtb_m.end(2):]
                write_text(ncx_path, new_ncx)
                print(f"\n  Repaired: NCX dtb:uid synced to OPF unique-identifier",
                      file=sys.stderr)
            else:
                # Inject dtb:uid into <head>
                new_ncx = re.sub(
                    r'(</head>)',
                    f'  <meta name="dtb:uid" content="{uid_val}"/>\n\\1',
                    ncx, count=1, flags=re.IGNORECASE)
                if new_ncx != ncx:
                    write_text(ncx_path, new_ncx)
                    print(f"\n  Repaired: NCX dtb:uid injected", file=sys.stderr)
        except Exception as e:
            _warn("NCX uid sync failed", opf_path, e)


def validate_language_consistency(temp_dir, verbose=False):
    # Cross-validate dc:language in OPF against xml:lang/lang on HTML <html> elements.
    # Mismatches cause TTS engines on Kobo/Kindle to use the wrong language model.
    # This is WARNING-only: we don't auto-repair since the OPF may be intentionally
    # different (e.g. an English book with a Latin foreword).
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            opf = read_text(opf_path)
            lang_m = re.search(r'<dc:language[^>]*>([^<]+)</dc:language>', opf, re.IGNORECASE)
            if not lang_m: continue
            opf_lang = _normalize_language(lang_m.group(1).strip())

            html_lang = _infer_language_from_html(temp_dir)
            if not html_lang: continue
            html_lang_n = _normalize_language(html_lang)

            # Allow partial match: "en" matches "en-US", "en-GB", etc.
            opf_base  = opf_lang.split('-')[0].lower()
            html_base = html_lang_n.split('-')[0].lower()
            if opf_base != html_base:
                msg = (f"dc:language='{opf_lang}' but HTML xml:lang='{html_lang_n}' "
                       f"— TTS may use wrong language model")
                if verbose:
                    print(f"\n  Warning: {msg} in {os.path.basename(opf_path)}", file=sys.stderr)
                else:
                    print(f"\n  Warning: language mismatch in {os.path.basename(opf_path)}"
                          f" — use --verbose for details", file=sys.stderr)
        except Exception as e:
            _warn("language consistency check failed", opf_path, e)


def detect_orphaned_manifest_items(temp_dir, verbose=False):
    # Detect files on disk that exist in the EPUB directory but are absent from
    # the OPF manifest entirely.  These are dead-weight bytes and may confuse validators.
    # WARNING-only: we don't auto-delete since they might be intentionally unlisted.
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            opf = read_text(opf_path)
            # Collect all hrefs in the manifest
            manifest_hrefs = set()
            for href in re.findall(r'<item\s[^>]*\bhref="([^"]+)"', opf, re.IGNORECASE):
                abs_p = os.path.normpath(os.path.join(opf_dir, href.split('#')[0]))
                manifest_hrefs.add(abs_p)

            # Walk files under the OPF dir (skip META-INF and mimetype)
            orphans = []
            for root, dirs, files in os.walk(opf_dir):
                # Skip hidden / system dirs
                dirs[:] = [d for d in dirs if not d.startswith('.')]
                for fname in files:
                    fp  = os.path.join(root, fname)
                    ext = os.path.splitext(fname)[1].lower()
                    # Only flag content-type files (images, CSS, HTML, fonts)
                    if ext not in ('.html', '.xhtml', '.htm', '.css',
                                   '.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg',
                                   '.ttf', '.otf', '.woff', '.woff2', '.ncx'):
                        continue
                    if fp not in manifest_hrefs:
                        orphans.append(os.path.relpath(fp, opf_dir))

            if orphans:
                removed, skipped = [], []
                for rel in orphans:
                    fp = os.path.normpath(os.path.join(opf_dir, rel))
                    try:
                        os.remove(fp)
                        removed.append(rel)
                    except Exception as _re:
                        skipped.append(rel)
                if removed:
                    if verbose:
                        print(f"\n  Removed {len(removed)} orphaned file(s) not in OPF manifest:",
                              file=sys.stderr)
                        for o in removed[:10]: print(f"    {o}", file=sys.stderr)
                        if len(removed) > 10:
                            print(f"    … and {len(removed) - 10} more", file=sys.stderr)
                    else:
                        print(f"\n  Removed {len(removed)} orphaned file(s) not in OPF manifest",
                              file=sys.stderr)
                for rel in skipped:
                    _warn(f"could not remove orphaned manifest file: {rel}", opf_path)
        except Exception as e:
            _warn("orphaned manifest item detection failed", opf_path, e)


def check_cover_aspect_ratio(temp_dir):
    # Warn when the cover image has an unusual aspect ratio for e-readers.
    # E-readers display cover thumbnails assuming a ~6:9 portrait ratio.
    # Landscape or near-square covers look broken in library grids.
    if not HAS_PIL: return
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            opf = read_text(opf_path)
            # Find cover image href
            cover_href = None
            # EPUB3 properties="cover-image"
            m = re.search(r'<item\s[^>]*\bproperties="[^"]*cover-image[^"]*"[^>]*\bhref="([^"]+)"',
                          opf, re.IGNORECASE)
            if m: cover_href = m.group(1)
            # EPUB2 <meta name="cover" content="id">
            if not cover_href:
                meta_m = re.search(r'<meta\s[^>]*\bname="cover"\s[^>]*\bcontent="([^"]+)"',
                                   opf, re.IGNORECASE)
                if meta_m:
                    cover_id = meta_m.group(1)
                    id_m = re.search(
                        rf'<item\s[^>]*\bid="{re.escape(cover_id)}"[^>]*\bhref="([^"]+)"',
                        opf, re.IGNORECASE)
                    if id_m: cover_href = id_m.group(1)
            if not cover_href: continue
            cover_abs = os.path.normpath(os.path.join(opf_dir, cover_href))
            if not os.path.exists(cover_abs): continue
            with Image.open(cover_abs) as img:
                w, h = img.size
            if w == 0 or h == 0: continue
            ratio = w / h
            # Portrait expected: 0.5–0.8 (6:9 = 0.667). Warn outside 0.45–0.9.
            if ratio > 0.9:
                print(f"\n  Warning: cover image is {'landscape' if ratio > 1 else 'near-square'}"
                      f" ({w}×{h}, ratio={ratio:.2f}) — may display poorly in library grids",
                      file=sys.stderr)
            elif ratio < 0.45:
                print(f"\n  Warning: cover image is very tall/narrow"
                      f" ({w}×{h}, ratio={ratio:.2f}) — may be cropped on some readers",
                      file=sys.stderr)
        except Exception as e:
            _warn("cover aspect ratio check failed", opf_path, e)


def convert_fonts_to_woff2(temp_dir):
    # Convert TTF/OTF fonts to WOFF2 for ~30-40% additional size reduction.
    # WOFF2 is supported by all modern EPUB3 reading systems (Kobo, Apple Books,
    # most Android readers).  Older EPUB2/Kindle readers fall back to the
    # original format gracefully since we only convert in EPUB3 books.
    # Requires fonttools + brotli.
    if not HAS_FONTTOOLS: return
    if not HAS_BROTLI: return  # brotli needed for woff2 encode

    # Only run for EPUB3 books (WOFF2 not reliably supported in EPUB2)
    opf_paths = find_opf(temp_dir)
    if not opf_paths: return
    try:
        opf = read_text(opf_paths[0])
        ver_m = re.search(r'<package[^>]*\bversion="([^"]+)"', opf, re.IGNORECASE)
        if not (ver_m and ver_m.group(1).startswith('3')): return
    except Exception: return

    from fontTools.ttLib import TTFont

    renamed_fonts = {}
    for root, _, files in os.walk(temp_dir):
        for fname in files:
            ext = os.path.splitext(fname)[1].lower()
            if ext not in ('.ttf', '.otf'): continue
            src_path = os.path.join(root, fname)
            size_kb  = os.path.getsize(src_path) / 1024
            if size_kb < FONT_SUBSET_MIN_KB: continue  # too small to bother
            dst_name = os.path.splitext(fname)[0] + '.woff2'
            dst_path = os.path.join(root, dst_name)
            try:
                tt = TTFont(src_path)
                tt.flavor = 'woff2'
                tt.save(dst_path)
                tt.close()
                size_after = os.path.getsize(dst_path) / 1024
                saving_pct = (1 - size_after / size_kb) * 100 if size_kb else 0
                if saving_pct > 0:
                    os.remove(src_path)
                    renamed_fonts[fname] = dst_name
                    print(f"\n  Note: converted {fname} → woff2"
                          f" {size_kb:.0f}→{size_after:.0f} KB ({saving_pct:.0f}% smaller)",
                          file=sys.stderr)
                else:
                    # Conversion made it larger — keep original
                    try: os.remove(dst_path)
                    except OSError: pass
            except Exception as e:
                _warn(f"WOFF2 conversion failed for {fname}", '', e)
                try: os.remove(dst_path)
                except OSError: pass

    if not renamed_fonts: return
    # Update OPF manifest media-types and hrefs
    for opf_path in opf_paths:
        try:
            content = read_text(opf_path)
            for old, new in renamed_fonts.items():
                content = content.replace(old, new)
                content = re.sub(
                    rf'(href="[^"]*{re.escape(new)}"[^>]*media-type=")(?:font/ttf|font/otf|'
                    rf'application/x-font-ttf|application/x-font-opentype)(")',
                    r'\1font/woff2\2', content, flags=re.IGNORECASE)
            write_text(opf_path, content)
        except Exception as e:
            _warn("OPF font manifest update failed", opf_path, e)
    # Update CSS @font-face src references
    for css_path in find_css(temp_dir):
        try:
            css = read_text(css_path)
            changed = False
            for old, new in renamed_fonts.items():
                if old in css:
                    css = css.replace(old, new)
                    changed = True
            if changed: write_text(css_path, css)
        except Exception as e:
            _warn("CSS font reference update failed", css_path, e)


def inject_accessibility_metadata(temp_dir):
    # Inject EPUB Accessibility 1.1 schema: metadata into EPUB3 OPF files that
    # lack it. These fields inform reading systems and screen readers about what
    # access modes are present, enabling users to choose appropriate reading tools.
    #
    # Inferred automatically from book content:
    #   accessMode: "textual" always (every book has text)
    #               "visual"  if the book contains images
    #   accessibilityFeature: "alternativeText" if all img tags have non-empty alt=
    #   accessibilitySummary: generic text (set conservatively)
    #   accessibilityHazard: "none" (most books have no hazards)
    #
    # Only runs for EPUB3 books. Never overwrites existing accessibility metadata.
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            opf = read_text(opf_path)
            ver_m = re.search(r'<package[^>]*\bversion="([^"]+)"', opf, re.IGNORECASE)
            if not (ver_m and ver_m.group(1).startswith('3')): continue

            # Skip if any accessibility metadata already present
            if re.search(r'property="schema:access', opf, re.IGNORECASE): continue

            # Determine access modes from content
            has_images = any(
                re.search(r'<img\b', read_text(p), re.IGNORECASE)
                for p in find_html(temp_dir)
                if p not in set(find_nav(temp_dir))
            )

            # Check if all images have meaningful alt text
            all_have_alt = True
            if has_images:
                html_cache = get_html_text_cache(temp_dir)
                nav_set = set(find_nav(temp_dir))
                for p, content in html_cache.items():
                    if p in nav_set: continue
                    for img_tag in re.findall(r'<img\b[^>]*/?>',  content, re.IGNORECASE):
                        alt_m = re.search(r'\balt="([^"]*)"', img_tag, re.IGNORECASE)
                        if not alt_m or not alt_m.group(1).strip():
                            all_have_alt = False
                            break
                    if not all_have_alt: break

            access_modes = ['textual']
            if has_images: access_modes.append('visual')

            new_metas = []
            for mode in access_modes:
                new_metas.append(
                    f'    <meta property="schema:accessMode">{mode}</meta>')
            if has_images and all_have_alt:
                new_metas.append(
                    '    <meta property="schema:accessibilityFeature">alternativeText</meta>')
            new_metas.append(
                '    <meta property="schema:accessibilityHazard">none</meta>')
            new_metas.append(
                '    <meta property="schema:accessibilitySummary">'
                'This publication includes basic accessibility metadata.</meta>')

            block = '\n' + '\n'.join(new_metas)
            new_opf = re.sub(r'(</metadata>)', block + '\n  \\1', opf,
                             count=1, flags=re.IGNORECASE)
            if new_opf != opf:
                write_text(opf_path, new_opf)
                print(f"\n  Note: injected EPUB accessibility metadata"
                      f" (accessMode: {', '.join(access_modes)})", file=sys.stderr)
        except Exception as e:
            _warn("accessibility metadata injection failed", opf_path, e)


def audit_manifest_properties(temp_dir):
    # EPUB3 requires certain manifest item properties= attributes:
    #   • properties="svg"     for documents containing inline <svg> elements
    #   • properties="mathml"  for documents containing inline MathML
    #   • properties="scripted" for documents using JavaScript (we strip JS, so
    #     this property should be absent — we check and remove it if present)
    #
    # Missing properties don't cause hard failures on most readers but can affect
    # rendering decisions (e.g. Kobo uses "svg" to decide rendering engine).
    # We auto-inject missing properties and remove stale ones.
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            opf = read_text(opf_path)
            ver_m = re.search(r'<package[^>]*\bversion="([^"]+)"', opf, re.IGNORECASE)
            if not (ver_m and ver_m.group(1).startswith('3')): continue

            id_to_href = _parse_manifest_items(opf)
            changed = False

            for item_id, href in id_to_href.items():
                ext = os.path.splitext(href)[1].lower()
                if ext not in ('.html', '.xhtml', '.htm'): continue
                abs_path = os.path.normpath(os.path.join(opf_dir, href))
                if not os.path.exists(abs_path): continue

                try:
                    content = read_text(abs_path)
                except Exception: continue

                needs_svg    = bool(re.search(r'<svg\b', content, re.IGNORECASE))
                needs_mathml = bool(re.search(r'<math\b', content, re.IGNORECASE))

                # Find this item tag in OPF
                item_pat = re.compile(
                    rf'<item\s[^>]*\bid="{re.escape(item_id)}"[^>]*/\s*>',
                    re.IGNORECASE)
                item_m = item_pat.search(opf)
                if not item_m: continue
                tag = item_m.group(0)

                prop_m = re.search(r'\bproperties="([^"]*)"', tag, re.IGNORECASE)
                props  = set((prop_m.group(1) if prop_m else '').split())

                old_props = set(props)
                if needs_svg:    props.add('svg')
                if needs_mathml: props.add('mathml')
                props.discard('scripted')   # we strip JS; never keep this flag

                if props == old_props: continue

                # Rewrite the item tag
                new_props_str = ' '.join(sorted(props))
                if new_props_str:
                    if prop_m:
                        new_tag = tag.replace(prop_m.group(0), f'properties="{new_props_str}"')
                    else:
                        new_tag = re.sub(r'(\s*/?\s*>)$',
                                         f' properties="{new_props_str}"\\1', tag)
                else:
                    # Remove empty properties=""
                    new_tag = re.sub(r'\s*\bproperties="[^"]*"', '', tag)

                opf = opf.replace(tag, new_tag)
                changed = True

            if changed: write_text(opf_path, opf)
        except Exception as e:
            _warn("manifest properties audit failed", opf_path, e)


def strip_link_type_attributes(temp_dir):
    # Remove redundant type="text/css" from <link rel="stylesheet"> tags.
    # In EPUB3 / XHTML5 the type attribute is unnecessary and considered noise.
    # Leaves type attributes on non-stylesheet links untouched.
    _LINK_TYPE_RE = re.compile(
        r'(<link\b[^>]*\brel="stylesheet"[^>]*?)\s+type="text/css"([^>]*/?>)',
        re.IGNORECASE
    )
    for html_path in find_html(temp_dir):
        try:
            html = read_text(html_path)
            new  = _LINK_TYPE_RE.sub(r'\1\2', html)
            write_if_changed(html_path, html, new)
        except Exception as e:
            _warn("link type attribute stripping failed", html_path, e)


def detect_rtl_issues(temp_dir, verbose=False):
    # Detect RTL (right-to-left) script usage and check whether the OPF spine
    # and HTML <html> elements have correct direction attributes.
    #
    # RTL languages detected: Arabic, Hebrew, Persian (Farsi), Urdu.
    # If RTL text is found but page-progression-direction="rtl" is absent from
    # the OPF spine, or dir="rtl" is absent from HTML, we warn.
    #
    # This is WARNING-only: writing the correct spine direction requires knowing
    # the intended reading order which we can't infer reliably from text alone.
    RTL_RANGE_RE = re.compile(
        r'[\u0600-\u06FF\u0590-\u05FF\u0750-\u077F\uFB50-\uFDFF\uFE70-\uFEFF]'
    )
    # Minimum fraction of text characters that must be RTL to trigger the check.
    # A threshold of 0.02 (2%) avoids false positives from books that merely
    # quote or title-reference Arabic/Hebrew text (e.g. a book about language
    # history quoting Arabic script) while still catching genuinely bilingual
    # or RTL-primary books.
    RTL_DENSITY_THRESHOLD = 0.02
    for opf_path in find_opf(temp_dir):
        opf_dir = os.path.dirname(opf_path)
        try:
            opf = read_text(opf_path)
            # Check OPF dc:language for known RTL languages
            lang_m = re.search(r'<dc:language[^>]*>([^<]+)</dc:language>', opf, re.IGNORECASE)
            opf_lang = (lang_m.group(1).strip()[:2].lower() if lang_m else '')
            known_rtl_langs = frozenset(['ar', 'he', 'fa', 'ur', 'yi', 'dv', 'ps'])
            lang_is_rtl = opf_lang in known_rtl_langs

            # Scan HTML for RTL codepoints — require density threshold to avoid
            # false positives from books that merely quote RTL script
            html_has_rtl = False
            html_cache = get_html_text_cache(temp_dir)
            nav_set = set(find_nav(temp_dir))
            total_chars = rtl_chars = 0
            for p, content in html_cache.items():
                if p in nav_set: continue
                # Strip tags to count only text characters
                text_only = re.sub(r'<[^>]+>', '', content)
                total_chars += len(text_only)
                rtl_chars += len(RTL_RANGE_RE.findall(text_only))
            density = rtl_chars / total_chars if total_chars > 0 else 0
            html_has_rtl = density >= RTL_DENSITY_THRESHOLD

            if not (lang_is_rtl or html_has_rtl): continue

            # Check OPF spine for page-progression-direction
            has_spine_rtl = bool(re.search(
                r'<spine\b[^>]*\bpage-progression-direction="rtl"', opf, re.IGNORECASE))
            # Check HTML html elements for dir="rtl"
            has_html_dir_rtl = any(
                re.search(r'<html\b[^>]*\bdir="rtl"', read_text(p), re.IGNORECASE)
                for p in find_html(temp_dir)
                if p not in nav_set
            )

            issues = []
            if not has_spine_rtl:
                issues.append('OPF spine missing page-progression-direction="rtl"')
            if not has_html_dir_rtl:
                issues.append('HTML <html> elements missing dir="rtl"')

            if issues:
                reason = f"RTL language detected (lang={opf_lang or 'from text'})"
                if verbose:
                    for issue in issues:
                        print(f"\n  Warning: {reason}: {issue}", file=sys.stderr)
                else:
                    print(f"\n  Warning: {reason} — RTL direction attributes may be missing"
                          f" ({len(issues)} issue(s)); use --verbose for details",
                          file=sys.stderr)
        except Exception as e:
            _warn("RTL direction check failed", opf_path, e)



def run_auto_repairs(temp_dir, verbose=False):
    # Phase 1: structural foundations
    repair_mimetype(temp_dir)
    repair_container_xml(temp_dir)
    repair_absolute_hrefs(temp_dir)
    normalize_href_case(temp_dir)             # case-sensitivity normalization
    repair_relative_paths(temp_dir)           # Phase 2: wrong-depth relative paths
    # Phase 2: manifest integrity (Bug #6 fix: run before validate_structure)
    repair_duplicate_manifest_ids(temp_dir)
    repair_duplicate_manifest_hrefs(temp_dir) # duplicate href detection
    repair_manifest_media_types(temp_dir)     # Phase 3: also injects missing media-type
    validate_font_integrity(temp_dir)         # @font-face / MIME validation
    repair_opf_metadata(temp_dir)             # Phase 3: language inference from HTML
    validate_unique_identifier(temp_dir)      # unique-identifier check
    repair_ncx_uid_sync(temp_dir)             # sync NCX dtb:uid ↔ OPF unique-identifier
    detect_circular_spine(temp_dir)           # Phase 3: remove non-content spine items
    repair_ncx_play_order(temp_dir)
    repair_nav_in_spine(temp_dir)
    repair_spine_linear_attributes(temp_dir)
    repair_duplicate_ids(temp_dir)
    validate_fragment_links(temp_dir)         # Phase 2: dead fragment removal
    audit_manifest_properties(temp_dir)       # inject missing svg/mathml properties (EPUB3)
    check_cross_document_id_uniqueness(temp_dir, verbose=verbose)
    detect_block_in_paragraph(temp_dir, verbose=verbose)
    detect_toc_heading_drift(temp_dir, verbose=verbose)
    audit_orphaned_ids(temp_dir, verbose=verbose)
    validate_language_consistency(temp_dir, verbose=verbose)  # dc:lang ↔ HTML lang
    detect_orphaned_manifest_items(temp_dir, verbose=verbose) # files not in manifest
    detect_rtl_issues(temp_dir, verbose=verbose)              # RTL direction check
    check_cover_aspect_ratio(temp_dir)                        # cover aspect ratio warn


def validate_structure(temp_dir):
    # Structural validation: raises StructuralValidationError on fatal problems,
    # prints warnings for non-fatal spec violations.
    #
    # Fatal checks (abort processing):
    #   • mimetype file present
    #   • META-INF/container.xml present and has rootfile full-path
    #   • OPF file exists at declared path
    #   • All manifest items reference existing files
    #   • Spine contains at least one itemref
    #   • All spine idrefs resolve to manifest items
    #
    # Non-fatal OPF metadata checks (warnings only — book still usable):
    #   • Exactly one dc:title (duplicate titles confuse reading systems)
    #   • Exactly one dc:language (duplicate languages are invalid per OPF spec)
    #   • At least one dc:identifier
    #   • package unique-identifier attribute matches an existing id= in metadata
    mimetype_path = os.path.join(temp_dir, 'mimetype')
    if not os.path.exists(mimetype_path): raise StructuralValidationError("Missing mimetype file")
    container_path = os.path.join(temp_dir, 'META-INF', 'container.xml')
    if not os.path.exists(container_path):
        raise StructuralValidationError("Missing META-INF/container.xml")
    container = read_text(container_path)
    rootfile  = re.search(r'full-path="([^"]+)"', container, re.IGNORECASE)
    if not rootfile: raise StructuralValidationError("container.xml has no rootfile full-path")
    opf_path = os.path.normpath(os.path.join(temp_dir, rootfile.group(1)))
    if not os.path.exists(opf_path):
        raise StructuralValidationError(f"OPF file not found: {rootfile.group(1)}")
    opf        = read_text(opf_path)
    opf_dir    = os.path.dirname(opf_path)
    id_to_href = _parse_manifest_items(opf)
    for item_id, href in id_to_href.items():
        item_path = os.path.normpath(os.path.join(opf_dir, href.split('#')[0]))
        if not os.path.exists(item_path):
            raise StructuralValidationError(
                f"Manifest item '{item_id}' references missing file: {href}")
    spine_items = re.findall(r'<itemref\s[^>]*\bidref="([^"]+)"', opf, re.IGNORECASE)
    if not spine_items: raise StructuralValidationError("Spine contains no itemref elements")
    for idref in spine_items:
        if idref not in id_to_href:
            raise StructuralValidationError(f"Spine itemref idref='{idref}' not found in manifest")

    # ── Non-fatal OPF metadata completeness checks ────────────────────────────
    titles      = re.findall(r'<dc:title\b[^>]*>[^<]*</dc:title>', opf, re.IGNORECASE)
    languages   = re.findall(r'<dc:language\b[^>]*>[^<]*</dc:language>', opf, re.IGNORECASE)
    identifiers = re.findall(r'<dc:identifier\b[^>]*>[^<]*</dc:identifier>', opf, re.IGNORECASE)

    if len(titles) > 1:
        _warn(f"OPF has {len(titles)} dc:title elements (should be exactly 1)", opf_path)
    if not titles:
        _warn("OPF missing dc:title", opf_path)
    if len(languages) > 1:
        _warn(f"OPF has {len(languages)} dc:language elements (should be exactly 1)", opf_path)
    if not identifiers:
        _warn("OPF missing dc:identifier (required by spec)", opf_path)

    # Check that unique-identifier points to an existing id= attribute
    uid_attr = re.search(r'<package\b[^>]*\bunique-identifier="([^"]+)"', opf, re.IGNORECASE)
    if uid_attr:
        uid_val  = uid_attr.group(1)
        id_attrs = set(re.findall(r'\bid="([^"]+)"', opf, re.IGNORECASE))
        if uid_val not in id_attrs:
            _warn(f"package unique-identifier=\"{uid_val}\" has no matching id= in metadata",
                  opf_path)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 14 — MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def process_epub(input_path, output_path,
                 remove_fields=None,
                 normalize_authors=False,
                 demote_secondary_authors=False,
                 normalize_publisher=False,
                 strip_publisher_legal=False,
                 consolidate_publisher=False,
                 clean_publisher_extra=False,
                 normalize_titles=False,
                 clean_title_tags=False,
                 normalize_subjects=False,
                 max_subjects=5,
                 enforce_modified_date=False,
                 enforce_single_identifier=False,
                 clean_description_html=False,
                 normalize_marc_roles=False,
                 scrub_print=False,
                 strip_deprecated_attrs=False,
                 strip_bg_colors=False,
                 kindle_mode=False,
                 inject_justify=False,
                 convert_font_units=False,
                 do_subset_fonts=True,
                 do_convert_woff2=True,
                 do_inject_accessibility=True,
                 verbose=False):
    # Full pipeline: metadata clean → optimise → validate → repackage.
    temp_dir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(input_path, 'r') as zf: zf.extractall(temp_dir)

        # Build the per-EPUB file index (single os.walk for the whole pipeline).
        # invalidate_epub_index() is called after any step that creates new files.
        build_epub_index(temp_dir)

        #  0. Normalise encodings
        normalise_encoding(temp_dir)

        #  1. Remove bloat
        remove_bloat(temp_dir)

        #  1b. Strip JavaScript (Feature F — always safe)
        strip_javascript(temp_dir)

        #  1d. Extract base64 data-URI images from CSS and HTML
        #      Must run early — before process_images and remove_unused_images
        #      so that extracted files go through the full image pipeline.
        extract_datauri_images(temp_dir)

        #  2a. Infer and inject cover metadata when missing
        inject_cover_metadata(temp_dir)

        #  2b. Generate EPUB3 NAV from NCX / headings if missing
        generate_epub3_nav(temp_dir)
        invalidate_epub_index(temp_dir)  # may have created a new .xhtml nav file

        #  3. NCX/NAV deduplication
        cleanup_nav(temp_dir)

        #  4. Trim TOC depth
        trim_toc_depth(temp_dir)

        #  4b. Rebuild degenerate TOC
        repair_degenerate_toc(temp_dir)

        #  5. Inject semantic landmarks
        inject_landmarks(temp_dir)

        #  6. XML-aware metadata cleaning
        meta_changes = clean_metadata(
            temp_dir,
            remove_fields=remove_fields,
            normalize_authors=normalize_authors,
            demote_secondary_authors=demote_secondary_authors,
            normalize_publisher=normalize_publisher,
            strip_publisher_legal=strip_publisher_legal,
            consolidate_publisher=consolidate_publisher,
            clean_publisher_extra=clean_publisher_extra,
            normalize_titles=normalize_titles,
            clean_title_tags=clean_title_tags,
            normalize_subjects=normalize_subjects,
            max_subjects=max_subjects,
            enforce_modified_date=enforce_modified_date,
            enforce_single_identifier=enforce_single_identifier,
            clean_description_html=clean_description_html,
            normalize_marc_roles=normalize_marc_roles,
            verbose=verbose,
            epub_name=os.path.basename(input_path),
        )

        #  6a. Sync NAV <title> element to cleaned OPF dc:title
        #  (NAV was generated at step 2b using pre-cleaned metadata — fix it now)
        _sync_nav_title_after_clean(temp_dir)

        #  6b. OPF <guide> repair
        repair_guide_references(temp_dir)

        #  7. Remove duplicate spine items
        remove_duplicate_spine_items(temp_dir)

        #  7b. Detect and mark empty/near-empty spine documents
        detect_empty_spine_documents(temp_dir)

        #  7c. Preserve / upgrade Calibre series metadata → EPUB3
        preserve_series_metadata(temp_dir)

        #  7d. Split XHTML files larger than XHTML_SPLIT_KB at heading boundaries
        split_large_xhtml(temp_dir)
        invalidate_epub_index(temp_dir)  # may have created new part .xhtml files

        #  7e. Warn about files that are STILL oversized after splitting
        #      (files with no heading split points, or where split produced large parts)
        warn_large_html_files(temp_dir)

        #  8. Remove unused images
        remove_unused_images(temp_dir)

        #  8b. Deduplicate identical images by content hash
        deduplicate_images(temp_dir)

        #  9. Deduplicate fonts
        deduplicate_fonts(temp_dir)

        # 10a. Subset embedded fonts to used codepoints (requires fontTools)
        if do_subset_fonts: subset_fonts(temp_dir)

        # 10b. Convert TTF/OTF → WOFF2 for additional size reduction (EPUB3 only)
        if do_convert_woff2: convert_fonts_to_woff2(temp_dir)

        # 11. Flatten stylesheets
        flatten_stylesheets(temp_dir)

        # 11b. Consolidate identical per-chapter CSS files (file-level dedup)
        consolidate_identical_css(temp_dir)

        # 11c. Extract repeated identical inline <style> blocks → shared CSS file
        deduplicate_inline_styles(temp_dir)
        invalidate_epub_index(temp_dir)   # may have added a new .css file

        # 12–16b. CSS transforms — single pass per file.
        # remove_unused_css reads all HTML first (cross-file) so runs before.
        # fix_render_blocking inlines @imports (may remove files) so runs before.
        # inject_justify_css targets a single file; stays as a separate call.
        remove_unused_css(temp_dir)
        fix_render_blocking(temp_dir)
        apply_css_transforms(
            temp_dir,
            kindle_mode=kindle_mode,
            do_convert_font_units=convert_font_units,
            do_strip_bg=strip_bg_colors,
        )
        if inject_justify: inject_justify_css(temp_dir)  # 15b. opt-in, single-target
        # 17–22c. HTML transforms — single pass per file.
        # Covers: charset, lang, typography, alt-text, lazy-loading, normalize,
        # address sanitization, empty-span removal, xml:space, !important inline,
        # and opt-in deprecated-attr stripping and print-artifact scrubbing.
        # body/html background-color stripping is CSS-side; handled in apply_css_transforms.
        # repair_xhtml_wellformedness must run last; kept as a separate final step.
        apply_html_transforms(
            temp_dir,
            do_strip_deprecated_attrs=strip_deprecated_attrs,
            do_scrub_print=scrub_print,
        )

        # 22d. XHTML well-formedness repair — must remain the final HTML step
        repair_xhtml_wellformedness(temp_dir)

        # 22e. Optimize SVG files — before minify which runs last
        optimize_svg_files(temp_dir)

        # 22f. Strip redundant type="text/css" from <link> tags (EPUB3 noise)
        strip_link_type_attributes(temp_dir)

        # 22g. Inject EPUB3 accessibility metadata (schema:accessMode etc.)
        if do_inject_accessibility: inject_accessibility_metadata(temp_dir)

        # 22h. Inject missing <title> elements into HTML <head> (screen readers + chapter menus)
        inject_missing_html_title(temp_dir)

        # 22i. Inject epub:type="bodymatter chapter" on unannotated EPUB3 body elements
        inject_epub_type_on_body(temp_dir)

        # 22j. Enforce cover spine item is linear="yes" so it appears in linear reading
        enforce_cover_spine_linear(temp_dir)

        # 23. Minify text files
        minify_text_files(temp_dir)

        # 24. Compress/resize images (WebP support added in v2 via IMAGE_EXTENSIONS)
        renamed = process_images(temp_dir)

        # 25. Update refs for PNG→JPG / WebP→JPG / GIF→PNG renames
        update_manifest_media_types(temp_dir, renamed)
        update_html_image_references(temp_dir, renamed)

        # 25b. Sync img width/height attrs to actual post-resize dimensions
        sync_image_dimensions(temp_dir)

        # 26. Manifest cleanup
        cleanup_manifest(temp_dir)

        # 27. Structural auto-repair (Bug #6 fix: new repair functions registered here)
        run_auto_repairs(temp_dir, verbose=verbose)

        # 28. Structural validation
        validate_structure(temp_dir)

        # 29. Repackage (small files stored uncompressed for faster access)
        # Write to a temp file first — rename atomically on success so a crash
        # mid-write never leaves a corrupt output file at output_path.
        tmp_output_fd, tmp_output_path = tempfile.mkstemp(
            suffix='.epub.tmp', dir=os.path.dirname(os.path.abspath(output_path)))
        os.close(tmp_output_fd)
        try:
            with zipfile.ZipFile(tmp_output_path, 'w') as new_zip:
                mt = os.path.join(temp_dir, 'mimetype')
                if not os.path.exists(mt):
                    # Create missing mimetype file — required by EPUB spec as first entry
                    with open(mt, 'w', encoding='ascii') as _mt_f:
                        _mt_f.write('application/epub+zip')
                new_zip.write(mt, 'mimetype', compress_type=zipfile.ZIP_STORED)
                for root, dirs, files in os.walk(temp_dir):
                    dirs.sort()   # deterministic directory traversal order
                    for file in sorted(files):
                        fp      = os.path.join(root, file)
                        arcname = os.path.relpath(fp, temp_dir)
                        if arcname == 'mimetype': continue
                        ext  = os.path.splitext(file)[1].lower()
                        size = os.path.getsize(fp)
                        if ext in _ALWAYS_DEFLATE_EXTS or size > ZIP_STORED_MAX_BYTES:
                            new_zip.write(fp, arcname,
                                          compress_type=zipfile.ZIP_DEFLATED,
                                          compresslevel=ZIP_LEVEL)
                        else: new_zip.write(fp, arcname, compress_type=zipfile.ZIP_STORED)
            # Atomic rename: replaces output_path only after successful write
            shutil.move(tmp_output_path, output_path)
        except Exception:
            try: os.unlink(tmp_output_path)
            except OSError: pass
            raise

        before_kb = os.path.getsize(input_path)  / 1024
        after_kb  = os.path.getsize(output_path) / 1024
        return before_kb, after_kb, meta_changes

    finally:
        invalidate_epub_index(temp_dir)  # release cached paths for this epub
        shutil.rmtree(temp_dir, ignore_errors=True)


def _worker(args):
    # Process one EPUB.  Always returns a 6-tuple:
    # (filename, before_kb, after_kb, meta_changes, error_str, log_lines)
    #
    # log_lines is a list of text lines captured from stdout/stderr during
    # processing (warnings, Meta: notices, etc.).  Returning them rather than
    # printing them directly ensures the main process can route them through
    # _ui_write() so they never bypass the terminal layout system.
    (filename, input_file, output_file, opts, verbose) = args

    # Redirect stdout and stderr to capture all incidental output.
    _old_out, _old_err = sys.stdout, sys.stderr
    _buf = io.StringIO()
    sys.stdout = sys.stderr = _buf

    try:
        try:
            check_drm(input_file)
        except DRMProtectedError as e:
            return filename, None, None, None, f"SKIPPED (DRM protected — {e})", []
        except ValueError as e:
            return filename, None, None, None, f"SKIPPED (invalid file — {e})", []
        try:
            before_kb, after_kb, meta_changes = process_epub(
                input_file, output_file, verbose=verbose, **opts)
            return (filename, before_kb, after_kb, meta_changes, None,
                    [l for l in _buf.getvalue().splitlines() if l.strip()])
        except StructuralValidationError as e:
            if os.path.exists(output_file): os.remove(output_file)
            return (filename, None, None, None, f"SKIPPED (validation failed — {e})",
                    [l for l in _buf.getvalue().splitlines() if l.strip()])
        except Exception as e:
            if os.path.exists(output_file): os.remove(output_file)
            return (filename, None, None, None, f"SKIPPED (error — {e})",
                    [l for l in _buf.getvalue().splitlines() if l.strip()])
    finally: sys.stdout, sys.stderr = _old_out, _old_err


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 15 — METADATA-ONLY MODE (standalone, in-place clean)
# ═══════════════════════════════════════════════════════════════════════════════

def _read_epub_opf_arcs(epub_path):
        # Returns (file_map, all_names, opf_arcs) or raises.
    with zipfile.ZipFile(epub_path, 'r') as zin:
        names    = zin.namelist()
        file_map = {name: zin.read(name) for name in names}

    container_raw = file_map.get("META-INF/container.xml")
    if not container_raw:
        raise ValueError("No META-INF/container.xml found")

    root = ET.fromstring(container_raw)
    opf_arcs = [
        rf.get("full-path")
        for rf in root.iter("{urn:oasis:names:tc:opendocument:xmlns:container}rootfile")
        if rf.get("full-path") in file_map
    ]
    if not opf_arcs:
        raise ValueError("Could not locate OPF in container.xml")
    return file_map, names, opf_arcs


def metadata_only_clean(epub_paths,
                        dry_run=False,
                        remove_fields=None,
                        normalize_authors=False,
                        demote_secondary_authors=False,
                        normalize_publisher=False,
                        strip_publisher_legal=False,
                        consolidate_publisher=False,
                        clean_publisher_extra=False,
                        normalize_titles=False,
                        clean_title_tags=False,
                        normalize_subjects=False,
                        max_subjects=5,
                        enforce_modified_date=False,
                        enforce_single_identifier=False,
                        clean_description_html=False,
                        normalize_marc_roles=False,
                        verbose=False):
    # Lightweight metadata-only path: extract OPF → clean → repack.
    # Does not touch images, CSS, HTML, or any content files.
    total_changes = 0
    for epub_path in epub_paths:
        epub_path = Path(epub_path)
        if not epub_path.exists(): print(f"Not found: {epub_path}"); continue
        if epub_path.suffix.lower() != '.epub': print(f"Skipping non-EPUB: {epub_path}"); continue

        print(f"\nProcessing: {epub_path.name}")

        # Check DRM before loading entire zip into memory (avoids wasting memory on DRM'd files)
        try:
            check_drm(epub_path)
        except DRMProtectedError as e: print(f"  SKIPPED ({e})"); continue
        except ValueError as e: print(f"  ERROR: {e}"); continue

        try:
            file_map, names, opf_paths_ = _read_epub_opf_arcs(epub_path)
        except zipfile.BadZipFile as e: print(f"  ERROR: {e}"); continue
        except (ET.ParseError, ValueError) as e: print(f"  ERROR: {e}"); continue

        meta_kwargs = {
            'remove_fields':             remove_fields,
            'normalize_authors':         normalize_authors,
            'demote_secondary_authors':  demote_secondary_authors,
            'normalize_publisher':       normalize_publisher,
            'strip_publisher_legal':     strip_publisher_legal,
            'consolidate_publisher':     consolidate_publisher,
            'clean_publisher_extra':     clean_publisher_extra,
            'normalize_titles':          normalize_titles,
            'clean_title_tags':          clean_title_tags,
            'normalize_subjects':        normalize_subjects,
            'max_subjects':              max_subjects,
            'enforce_modified_date':     enforce_modified_date,
            'enforce_single_identifier': enforce_single_identifier,
            'clean_description_html':    clean_description_html,
            'normalize_marc_roles':      normalize_marc_roles,
            'verbose':                   verbose,
        }

        book_changes = []
        modified_opfs = {}

        for opf_arc in opf_paths_:
            # Write OPF to a temp file, run the shared clean function, read back
            tmp_fd, tmp_path = tempfile.mkstemp(suffix='.opf')
            try:
                os.close(tmp_fd)
                with open(tmp_path, 'wb') as f: f.write(file_map[opf_arc])

                changes = clean_opf_metadata_xml(tmp_path, **meta_kwargs)

                if changes:
                    book_changes.extend(changes)
                    with open(tmp_path, 'rb') as f: modified_opfs[opf_arc] = f.read()
            finally:
                try:
                    os.unlink(tmp_path)
                except OSError: pass

        if not book_changes and not modified_opfs: print("  No changes needed."); continue

        print(f"  {len(book_changes)} field change(s):")
        for field, before, after in book_changes: print(f"    [{field}] '{before}' → '{after}'")

        if dry_run: print("  DRY RUN — file not modified."); continue


        for opf_arc, new_bytes in modified_opfs.items(): file_map[opf_arc] = new_bytes

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', compression=zipfile.ZIP_DEFLATED,
                             compresslevel=ZIP_LEVEL) as zout:
            if "mimetype" in file_map:
                zout.writestr(zipfile.ZipInfo("mimetype"), file_map["mimetype"],
                              compress_type=zipfile.ZIP_STORED)
            for name in names:
                if name == "mimetype": continue
                zout.writestr(name, file_map[name])
        new_bytes = buf.getvalue()

        # Atomic-safe write: copy original to .bak before overwriting.
        # On success the backup is removed.  On any failure the backup is
        # restored, leaving the source epub intact.
        backup_path = epub_path.with_suffix('.epub.bak')
        try:
            shutil.copy2(epub_path, backup_path)
            epub_path.write_bytes(new_bytes)
            with zipfile.ZipFile(epub_path, 'r') as _vz:
                bad = _vz.testzip()
                if bad:
                    raise zipfile.BadZipFile(f"zip integrity check failed: {bad}")
            backup_path.unlink()
            print(f"  Saved: {epub_path.name}")
        except Exception as _we:
            print(f"\n  ERROR writing {epub_path.name}: {_we}", file=sys.stderr)
            if backup_path.exists():
                try:
                    backup_path.rename(epub_path)
                    print(f"  Restored from backup: {epub_path.name}",
                          file=sys.stderr)
                except Exception as _re:
                    print(f"  WARNING: backup restore failed: {_re}\n"
                          f"  Backup preserved at: {backup_path}",
                          file=sys.stderr)
            continue
        total_changes += len(book_changes)

    print(f"\nDone. {total_changes} total field change(s).")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 15b — ANALYSIS MODES (read-only; no file mutation)
# ═══════════════════════════════════════════════════════════════════════════════
#
#  Stand-alone passes that inspect an EPUB and emit a report to stdout
#  without modifying the source file:
#
#    epub_stats(path)          — size/word/font breakdown (absorbs font analysis)
#    audit_accessibility(path) — WCAG/EPUB accessibility issues in HTML
#    diff_metadata(path)       — before/after field-change report (dry-run)
#
#  Each function accepts a plain .epub path string, extracts to a temp dir,
#  runs its analysis, then cleans up.  They share the same output style as the
#  rest of the toolkit (print to stdout/stderr, same Warning:/Note: prefixes).
# ═══════════════════════════════════════════════════════════════════════════════

def epub_stats(epub_path, verbose=False):
    # Print a size/content breakdown for a single EPUB without modifying it.
    #
    # Reports:
    # • Total file size and per-category asset breakdown (HTML, CSS, images,
    # fonts, other), both in KB and as a percentage of total.
    # • Approximate word count (text-node words across all spine HTML files).
    # • Image count and total image payload.
    # • Embedded font count and total font payload.
    # • Number of spine items, CSS files, and manifest items.
    #
    # Returns a dict of the computed statistics for programmatic use.
    temp_dir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(epub_path, 'r') as zf: zf.extractall(temp_dir)

        total_bytes    = os.path.getsize(epub_path)
        html_bytes     = css_bytes = img_bytes = font_bytes = other_bytes = 0
        html_files     = find_html(temp_dir)
        css_files      = find_css(temp_dir)
        word_count     = 0
        image_files    = []
        font_files_lst = []

        # Walk every extracted file and bin it by type
        for root, _, files in os.walk(temp_dir):
            for fname in files:
                fpath = os.path.join(root, fname)
                ext   = os.path.splitext(fname)[1].lower()
                size  = os.path.getsize(fpath)
                if ext in ('.html', '.xhtml', '.htm'): html_bytes += size
                elif ext == '.css': css_bytes += size
                elif ext in IMAGE_EXTENSIONS_WITH_SVG:
                    img_bytes += size
                    image_files.append(fpath)
                elif ext in FONT_EXTENSIONS:
                    font_bytes += size
                    font_files_lst.append(fpath)
                elif ext not in ('.opf', '.ncx', '.xml', '.mimetype', ''): other_bytes += size

        # Approximate word count — strip tags, split on whitespace
        _tag_strip = re.compile(r'<[^>]+>')
        for hp in html_files:
            try:
                text = _tag_strip.sub(' ', read_text(hp))
                word_count += len(text.split())
            except Exception: pass

        # Spine item count from OPF
        spine_count = 0
        for opf_path in find_opf(temp_dir):
            try:
                opf = read_text(opf_path)
                spine_count += len(re.findall(r'<itemref\s', opf, re.IGNORECASE))
            except Exception: pass

        total_kb  = total_bytes / 1024
        def _pct(b): return (b / total_bytes * 100) if total_bytes else 0
        def _kb(b):  return b / 1024

        stats = {
            'total_kb':    total_kb,
            'html_kb':     _kb(html_bytes),
            'css_kb':      _kb(css_bytes),
            'images_kb':   _kb(img_bytes),
            'fonts_kb':    _kb(font_bytes),
            'other_kb':    _kb(other_bytes),
            'word_count':  word_count,
            'image_count': len(image_files),
            'font_count':  len(font_files_lst),
            'html_count':  len(html_files),
            'css_count':   len(css_files),
            'spine_count': spine_count,
        }

        name = os.path.basename(epub_path)
        print(f"\n{'─'*60}")
        print(f"  Stats: {name}")
        print(f"{'─'*60}")
        print(f"  Total size   : {total_kb:>8.1f} KB")
        print(f"  HTML/XHTML   : {_kb(html_bytes):>8.1f} KB"
              f"  ({_pct(html_bytes):.1f}%)  {len(html_files)} files")
        print(f"  CSS          : {_kb(css_bytes):>8.1f} KB"
              f"  ({_pct(css_bytes):.1f}%)  {len(css_files)} files")
        print(f"  Images       : {_kb(img_bytes):>8.1f} KB"
              f"  ({_pct(img_bytes):.1f}%)  {len(image_files)} files")
        print(f"  Fonts        : {_kb(font_bytes):>8.1f} KB"
              f"  ({_pct(font_bytes):.1f}%)  {len(font_files_lst)} files")
        if other_bytes:
            print(f"  Other        : {_kb(other_bytes):>8.1f} KB  ({_pct(other_bytes):.1f}%)")
        print(f"  Spine items  : {spine_count}")
        print(f"  Word count   : ~{word_count:,}")
        # Font inventory — reuse the already-extracted temp_dir
        _font_inventory_report(temp_dir, verbose=verbose)
        return stats

    finally: shutil.rmtree(temp_dir, ignore_errors=True)


def audit_accessibility(epub_path, verbose=False):
    # Scan an EPUB for accessibility issues without modifying it.
    #
    # Checks (all read-only):
    # • Images missing alt attribute entirely.
    # • Images with empty alt="" that appear non-decorative (have a src name
    # suggesting real content — not 'spacer', 'divider', 'rule', etc.).
    # • Heading hierarchy skips (e.g. h1 → h3 with no h2 in between).
    # • Tables with no <th> header cells.
    # • HTML documents with no lang= / xml:lang= on <html>.
    # • Duplicate id= values within a single document.
    #
    # Returns a dict of {check_name: [list of findings]}.
    # Prints a summary to stdout; individual findings printed if verbose=True.
    temp_dir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(epub_path, 'r') as zf: zf.extractall(temp_dir)

        issues = {
            'missing_alt':      [],   # (file, img_src)
            'empty_alt':        [],   # (file, img_src) — probably non-decorative
            'heading_skip':     [],   # (file, from_level, to_level)
            'table_no_header':  [],   # (file,)
            'missing_lang':     [],   # (file,)
            'duplicate_ids':    [],   # (file, id_value, count)
        }

        _img_re       = re.compile(r'<img\b([^>]*)>', re.IGNORECASE)
        _alt_re       = re.compile(r'\balt="([^"]*)"', re.IGNORECASE)
        _src_re       = re.compile(r'\bsrc="([^"]*)"', re.IGNORECASE)
        _heading_re   = re.compile(r'<h([1-6])\b', re.IGNORECASE)
        _table_re     = re.compile(r'<table\b', re.IGNORECASE)
        _th_re        = re.compile(r'<th\b', re.IGNORECASE)
        _lang_re      = re.compile(r'<html\b[^>]*\blang=', re.IGNORECASE)
        _id_re        = re.compile(r'\bid="([^"]+)"', re.IGNORECASE)
        # Decorative image name hints — empty alt on these is fine
        _deco_re      = re.compile(
            r'(?:spacer|divider|rule|separator|ornament|fleuron|decoration|border)',
            re.IGNORECASE)

        nav_paths = set(find_nav(temp_dir))

        for html_path in find_html(temp_dir):
            if html_path in nav_paths: continue   # NAV structural links — not content
            fname = os.path.basename(html_path)
            try:
                html = read_text(html_path)
            except Exception: continue

            # ── lang attribute ───────────────────────────────────────────────
            if not _lang_re.search(html): issues['missing_lang'].append(fname)

            # ── images ──────────────────────────────────────────────────────
            for img_attrs in _img_re.findall(html):
                alt_m = _alt_re.search(img_attrs)
                src_m = _src_re.search(img_attrs)
                src   = src_m.group(1) if src_m else ''
                if alt_m is None: issues['missing_alt'].append((fname, src))
                elif alt_m.group(1) == '' and not _deco_re.search(src):
                    issues['empty_alt'].append((fname, src))

            # ── heading hierarchy ────────────────────────────────────────────
            levels = [int(m.group(1)) for m in _heading_re.finditer(html)]
            for i in range(1, len(levels)):
                if levels[i] > levels[i-1] + 1:
                    issues['heading_skip'].append(
                        (fname, levels[i-1], levels[i]))

            # ── tables without <th> ──────────────────────────────────────────
            for table_m in _table_re.finditer(html):
                # Find the end of this table by scanning forward
                table_start = table_m.start()
                table_end   = html.find('</table>', table_start)
                if table_end == -1: table_end = len(html)
                chunk = html[table_start:table_end]
                if not _th_re.search(chunk): issues['table_no_header'].append(fname)

            # ── duplicate IDs within this document ───────────────────────────
            id_counts = {}
            for id_val in _id_re.findall(html): id_counts[id_val] = id_counts.get(id_val, 0) + 1
            for id_val, count in id_counts.items():
                if count > 1: issues['duplicate_ids'].append((fname, id_val, count))

        name = os.path.basename(epub_path)
        total = sum(len(v) for v in issues.values())
        print(f"\n{'─'*60}")
        print(f"  Accessibility audit: {name}")
        print(f"{'─'*60}")

        labels = {
            'missing_alt':     'Images missing alt=',
            'empty_alt':       'Possibly non-decorative empty alt=',
            'heading_skip':    'Heading hierarchy skips',
            'table_no_header': 'Tables without <th>',
            'missing_lang':    'Documents missing lang attribute',
            'duplicate_ids':   'Duplicate id= values (within file)',
        }
        for key, label in labels.items():
            count = len(issues[key])
            if count:
                print(f"  ✗ {label}: {count}")
                if verbose:
                    for item in issues[key][:10]: print(f"      {item}")
                    if len(issues[key]) > 10: print(f"      … and {len(issues[key])-10} more")
            else: print(f"  ✓ {label}")

        if total == 0: print(f"  No accessibility issues found.")
        else: print(f"  Total issues: {total}  (use --verbose for details)")

        return issues

    finally: shutil.rmtree(temp_dir, ignore_errors=True)


def _font_inventory_report(temp_dir, verbose=False):
    # Print an embedded-font inventory for an already-extracted temp_dir.
    #
    # Called by epub_stats() (reuses its temp_dir without a second unzip) and
    # by analyse_fonts() (which extracts first).  Returns a result dict.

    font_paths = []
    for root, _, files in os.walk(temp_dir):
        for fname in files:
            if os.path.splitext(fname)[1].lower() in FONT_EXTENSIONS:
                font_paths.append(os.path.join(root, fname))

    _ff_url_re = re.compile(
        r'@font-face\s*\{[^}]*?src\s*:[^}]*?url\(\s*["\']*([^\s\'"\'\)]+)',
        re.IGNORECASE | re.DOTALL)
    css_font_refs = set()
    for css_path in find_css(temp_dir):
        try:
            css      = read_text(css_path)
            css_dir  = os.path.dirname(css_path)
            for href in _ff_url_re.findall(css):
                href = href.split('?')[0].split('#')[0]
                css_font_refs.add(os.path.normpath(os.path.join(css_dir, href)))
        except Exception: pass

    hash_map = {}
    for fp in font_paths:
        try:
            h = hashlib.sha256(read_raw(fp)).hexdigest()
            hash_map.setdefault(h, []).append(fp)
        except Exception: pass

    OVERSIZED_KB = 200
    result = {
        'embedded':   [],   # (basename, ext, size_kb)
        'missing':    [],   # CSS @font-face ref that has no file on disk
        'orphaned':   [],   # font file not referenced in any @font-face
        'duplicates': [],   # list of [name, name, …] groups with same SHA-256
        'oversized':  [],   # (basename, size_kb) > OVERSIZED_KB
    }
    for fp in font_paths:
        fname   = os.path.basename(fp)
        ext     = os.path.splitext(fname)[1].lower()
        size_kb = os.path.getsize(fp) / 1024
        result['embedded'].append((fname, ext, size_kb))
        if size_kb > OVERSIZED_KB: result['oversized'].append((fname, size_kb))
    for ref in css_font_refs:
        if not os.path.exists(ref): result['missing'].append(os.path.basename(ref))
    for fp in font_paths:
        if fp not in css_font_refs: result['orphaned'].append(os.path.basename(fp))
    for paths in hash_map.values():
        if len(paths) > 1: result['duplicates'].append([os.path.basename(p) for p in paths])

    if not result['embedded']: return result   # nothing to print

    total_kb = sum(s for _, _, s in result['embedded'])
    print(f"  Fonts ({len(result['embedded'])}, {total_kb:.1f} KB total):")
    for fname, ext, size_kb in sorted(result['embedded'], key=lambda x: -x[2]):
        flag = '  ⚠ oversized' if size_kb > OVERSIZED_KB else ''
        print(f"    {fname:<40s} {size_kb:>7.1f} KB  {ext}{flag}")
    if result['missing']:
        print(f"  ✗ @font-face references not on disk ({len(result['missing'])}):")
        for r in result['missing']: print(f"      {r}")
    if result['orphaned']:
        print(f"  ⚠ Font files not in any @font-face ({len(result['orphaned'])}):")
        for r in result['orphaned']: print(f"      {r}")
    if result['duplicates']:
        print(f"  ✗ Duplicate fonts ({len(result['duplicates'])} groups):")
        for g in result['duplicates']: print(f"      {' == '.join(g)}")
    return result


def analyse_fonts(epub_path, verbose=False):
    # Report on embedded fonts in an EPUB without modifying it.
    # Standalone entrypoint — use epub_stats() to get fonts as part of a
    # full-stats report without extracting the EPUB twice.
    temp_dir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(epub_path, 'r') as zf: zf.extractall(temp_dir)
        name = os.path.basename(epub_path)
        print(f"\n{'─'*60}")
        print(f"  Font analysis: {name}")
        print(f"{'─'*60}")
        return _font_inventory_report(temp_dir, verbose=verbose)
    finally: shutil.rmtree(temp_dir, ignore_errors=True)


def diff_metadata(epub_paths,
                  remove_fields=None,
                  normalize_authors=False,
                  demote_secondary_authors=False,
                  normalize_publisher=False,
                  strip_publisher_legal=False,
                  consolidate_publisher=False,
                  clean_publisher_extra=False,
                  normalize_titles=False,
                  clean_title_tags=False,
                  normalize_subjects=False,
                  max_subjects=5,
                  enforce_modified_date=False,
                  enforce_single_identifier=False,
                  clean_description_html=False,
                  normalize_marc_roles=False):
    # Show what metadata changes would be made by --metadata-only, without
    # writing anything.
    #
    # Runs the full clean_opf_metadata_xml() pipeline on each EPUB in dry-run
    # mode and prints a before/after diff for every field that would change.
    # Accepts the same options as --metadata-only.
    #
    # Returns a dict of {epub_name: [(field, before, after), ...]} changes.
    all_changes = {}

    meta_kwargs = {
        'remove_fields':             remove_fields,
        'normalize_authors':         normalize_authors,
        'demote_secondary_authors':  demote_secondary_authors,
        'normalize_publisher':       normalize_publisher,
        'strip_publisher_legal':     strip_publisher_legal,
        'consolidate_publisher':     consolidate_publisher,
        'clean_publisher_extra':     clean_publisher_extra,
        'normalize_titles':          normalize_titles,
        'clean_title_tags':          clean_title_tags,
        'normalize_subjects':        normalize_subjects,
        'max_subjects':              max_subjects,
        'enforce_modified_date':     enforce_modified_date,
        'enforce_single_identifier': enforce_single_identifier,
        'clean_description_html':    clean_description_html,
        'normalize_marc_roles':      normalize_marc_roles,
    }

    for epub_path in epub_paths:
        epub_path = Path(epub_path)
        if not epub_path.exists(): print(f"  Not found: {epub_path}"); continue
        if epub_path.suffix.lower() != '.epub': print(f"  Skipping non-EPUB: {epub_path}"); continue

        # DRM check before reading zip
        try:
            check_drm(epub_path)
        except DRMProtectedError as e: print(f"  SKIPPED {epub_path.name} ({e})"); continue

        try:
            file_map, _names, opf_arcs = _read_epub_opf_arcs(epub_path)
        except zipfile.BadZipFile as e: print(f"  ERROR reading {epub_path.name}: {e}"); continue
        except (ET.ParseError, ValueError) as e: print(f"  ERROR {epub_path.name}: {e}"); continue

        book_changes = []
        for opf_arc in opf_arcs:
            tmp_fd, tmp_path = tempfile.mkstemp(suffix='.opf')
            try:
                os.close(tmp_fd)
                with open(tmp_path, 'wb') as f: f.write(file_map[opf_arc])
                book_changes.extend(clean_opf_metadata_xml(tmp_path, **meta_kwargs))
            finally:
                try: os.unlink(tmp_path)
                except OSError: pass

        name = epub_path.name
        print(f"\n{'─'*60}")
        print(f"  Diff: {name}")
        print(f"{'─'*60}")
        if book_changes:
            for field, before, after in book_changes:
                print(f"  [{field}]")
                print(f"    before: {before!r}")
                print(f"    after : {after!r}")
        else: print("  No metadata changes would be made.")
        all_changes[name] = book_changes

    return all_changes

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 16 — CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    global PUBLISHER_FUZZY_CORRECT
    global _log_level, _log_file_handle, _warn_log_handle, _warn_count
    parser = argparse.ArgumentParser(
        description="EPUB Toolkit — optimizer + metadata cleaner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("files", nargs="*",
                        help="EPUB file(s) for --metadata-only, --dry-run, or analysis modes")

    # ── Modes ─────────────────────────────────────────────────────────────────
    parser.add_argument("--metadata-only", action="store_true",
                        help="Clean metadata in-place without touching images/CSS/HTML")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview metadata changes without writing (use with --metadata-only)")
    parser.add_argument("--stats", dest="do_stats",
                        action="store_true", default=False,
                        help="Report file size, word count, font inventory, and asset breakdown")
    parser.add_argument("--audit-accessibility", dest="do_audit_accessibility",
                        action="store_true", default=False,
                        help="Report accessibility issues: alt text, headings, lang, tables")

    # ── Run control ───────────────────────────────────────────────────────────
    parser.add_argument("-v", dest="verbose", action="store_true",
                        help="Verbose output (shorthand for --log-level verbose)")
    parser.add_argument("--log-level",
                        choices=['warning', 'info', 'note', 'verbose'],
                        default=None, metavar="LEVEL",
                        help="Log verbosity: warning | info | note (default) | verbose")
    parser.add_argument("--log-file", action="store_true", default=False,
                        help=f"Append timestamped session log to {LOG_FILE}")
    parser.add_argument("--no-subset-fonts", dest="subset_fonts",
                        action="store_false", default=True,
                        help="Disable font subsetting")
    parser.add_argument("--no-woff2", dest="convert_woff2",
                        action="store_false", default=True,
                        help="Disable TTF/OTF → WOFF2 conversion (EPUB3 only; "
                             "requires fonttools + brotli)")
    parser.add_argument("--no-accessibility-metadata", dest="inject_accessibility",
                        action="store_false", default=True,
                        help="Disable EPUB3 accessibility metadata injection "
                             "(schema:accessMode, schema:accessibilityHazard etc.)")
    parser.add_argument("--kindle", dest="kindle_mode", action="store_true", default=False,
                        help="Aggressive CSS cleanup: also strips position:absolute and float")

    # ── Opt-in content features ───────────────────────────────────────────────
    opt = parser.add_argument_group("opt-in content features (full pipeline only)")
    opt.add_argument("--inject-justify", dest="inject_justify",
                     action="store_true", default=False,
                     help="Inject text-align:justify into paragraph CSS")
    opt.add_argument("--convert-font-units", dest="convert_font_units",
                     action="store_true", default=False,
                     help="Convert pt/px font sizes to relative em")
    opt.add_argument("--max-subjects", type=int, default=MAX_SUBJECTS,
                     metavar="N", help=f"Max dc:subject tags to keep (default {MAX_SUBJECTS})")

    # ── Advanced overrides ────────────────────────────────────────────────────
    adv = parser.add_argument_group(
        "advanced overrides",
        "All passes are on by default. These flags disable individual categories "
        "or the whole metadata / HTML pipeline.")
    # Umbrella kills
    adv.add_argument("--no-clean-metadata", dest="no_clean_metadata",
                     action="store_true", default=False,
                     help="Disable ALL metadata normalisation")
    adv.add_argument("--no-clean-html", dest="no_clean_html",
                     action="store_true", default=False,
                     help="Disable ALL HTML/CSS cleanup")
    # Per-category metadata opt-outs
    adv.add_argument("--no-normalize-titles", dest="normalize_titles",
                     action="store_false", default=NORMALIZE_TITLES,
                     help="Skip title casing, tag-stripping and sort-key generation")
    adv.add_argument("--no-normalize-authors", dest="normalize_authors",
                     action="store_false", default=NORMALIZE_AUTHORS,
                     help="Skip author casing, inversion fix, sort-key, role normalisation "
                          "and secondary-author demotion")
    adv.add_argument("--no-normalize-publisher", dest="normalize_publisher",
                     action="store_false", default=NORMALIZE_PUBLISHER,
                     help="Skip publisher casing, legal-suffix stripping, imprint "
                          "consolidation and verbose-suffix removal")
    adv.add_argument("--no-normalize-subjects", dest="normalize_subjects",
                     action="store_false", default=NORMALIZE_SUBJECTS,
                     help="Skip subject deduplication, blocklist filtering and cap")
    # Power-user / field removal
    adv.add_argument("--publisher-fuzzy", dest="publisher_fuzzy",
                     action="store_true", default=PUBLISHER_FUZZY_CORRECT,
                     help="Auto-correct obvious publisher name typos "
                          "(e.g. 'HarprCollins' → 'HarperCollins')")
    adv.add_argument("--remove-fields", nargs="*", default=None, metavar="FIELD",
                     help="Delete specific metadata fields entirely, e.g. dc:rights")

    args = parser.parse_args()

    # ── Apply module globals from args ──────────────────────────────────────
    PUBLISHER_FUZZY_CORRECT = args.publisher_fuzzy

    # ── Umbrella overrides: --no-clean-metadata / --no-clean-html ────────────
    if args.no_clean_metadata:
        args.normalize_titles = args.normalize_authors = False
        args.normalize_publisher = args.normalize_subjects = False
    if args.no_clean_html:
        pass  # html opts all use their compile-time defaults; no per-field args remain

    # ── Per-category cascades ─────────────────────────────────────────────────
    # --no-normalize-authors also disables the sub-passes that logically depend on it
    _do_authors        = args.normalize_authors
    _do_demote         = _do_authors  # demoting secondary authors implies author normalisation
    _do_marc           = _do_authors  # MARC role normalisation is part of author normalisation
    # --no-normalize-publisher disables all publisher sub-passes
    _do_publisher      = args.normalize_publisher
    _do_strip_legal    = _do_publisher
    _do_consolidate    = _do_publisher
    _do_clean_extra    = _do_publisher
    # --no-normalize-titles disables title-tag stripping too
    _do_titles         = args.normalize_titles
    _do_title_tags     = _do_titles

    effective_remove = args.remove_fields if args.remove_fields is not None else REMOVE_FIELDS

    # ── Resolve log level ─────────────────────────────────────────────────────
    if args.verbose: _log_level = L_VERBOSE
    elif args.log_level: _log_level = LOG_LEVEL_NAMES[args.log_level]
    else: _log_level = L_INFO

    if args.log_file:
        try:
            _log_file_handle = open(LOG_FILE, 'a', encoding='utf-8')
            _log_file_handle.write(
                f"\n{'='*72}\n"
                f"Run started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                f"  log-level={('info','note','warning','verbose')[_log_level-1]}"
                f"  workers={MAX_WORKERS}\n"
                f"{'='*72}\n"
            )
            _log_file_handle.flush()
        except OSError as e:
            print(f"Warning: cannot open log file {LOG_FILE!r}: {e}", file=sys.stderr)
            _log_file_handle = None

    # Mirror _warn() output into the log file so warnings are captured even
    # when issued from worker threads or functions that don't use _ui_write.
    _warn_log_handle = _log_file_handle
    _warn_count = 0   # reset counter for this session

    # Bundle all metadata options into a dict for clean passing to worker
    meta_opts = dict(
        remove_fields=effective_remove,
        normalize_authors=_do_authors,
        demote_secondary_authors=_do_demote,
        normalize_marc_roles=_do_marc,
        normalize_publisher=_do_publisher,
        strip_publisher_legal=_do_strip_legal,
        consolidate_publisher=_do_consolidate,
        clean_publisher_extra=_do_clean_extra,
        normalize_titles=_do_titles,
        clean_title_tags=_do_title_tags,
        normalize_subjects=args.normalize_subjects,
        max_subjects=args.max_subjects,
        enforce_modified_date=ENFORCE_MODIFIED_DATE,
        enforce_single_identifier=ENFORCE_SINGLE_IDENTIFIER,
        clean_description_html=CLEAN_DESCRIPTION_HTML,
    )
    html_opts = dict(
        scrub_print=SCRUB_PRINT_ARTIFACTS,
        strip_deprecated_attrs=STRIP_DEPRECATED_HTML_ATTRS,
        strip_bg_colors=STRIP_BODY_BACKGROUND,
        kindle_mode=args.kindle_mode,
        inject_justify=args.inject_justify,
        convert_font_units=args.convert_font_units,
        do_subset_fonts=args.subset_fonts,
        do_convert_woff2=args.convert_woff2,
        do_inject_accessibility=args.inject_accessibility,
        # verbose is passed separately to process_epub to avoid duplicate kwarg
    )

    # ── Analysis modes: read-only, report and exit ────────────────────────────
    if args.do_stats or args.do_audit_accessibility:
        if not args.files: parser.error("Please provide one or more .epub files for analysis mode")
        epub_paths = []
        for pattern in args.files:
            epub_paths.extend(Path(".").glob(pattern) if "*" in pattern else [Path(pattern)])
        for ep in [str(p) for p in epub_paths]:
            if args.do_stats: epub_stats(ep, verbose=args.verbose)
            if args.do_audit_accessibility: audit_accessibility(ep, verbose=args.verbose)
        return

    if args.metadata_only:
        if not args.files:
            parser.error("Please provide one or more .epub files with --metadata-only")
        epub_paths = []
        for pattern in args.files:
            epub_paths.extend(Path(".").glob(pattern) if "*" in pattern else [Path(pattern)])
        metadata_only_clean(epub_paths, dry_run=args.dry_run, verbose=args.verbose, **meta_opts)
    else:
        if os.path.realpath(SOURCE_DIR) == os.path.realpath(OUTPUT_DIR):
            print("Error: SOURCE_DIR and OUTPUT_DIR must be different.", file=sys.stderr)
            sys.exit(1)
        if not os.path.isdir(SOURCE_DIR):
            print(f"Error: source directory not found: {SOURCE_DIR}", file=sys.stderr)
            sys.exit(1)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        epubs = [f for f in os.listdir(SOURCE_DIR) if f.lower().endswith('.epub')]
        if not epubs: print("No EPUB files found in source directory."); return

        # ── Run ─────────────────────────────────────────────────────────────
        _lock       = threading.Lock()
        _quit_flag  = threading.Event()
        _start_time = time.monotonic()

        # Ctrl+C: finish current book(s) then print summary
        def _sigint(sig, frame):
            if not _quit_flag.is_set():
                print("\n  Interrupted — finishing current book(s)…", flush=True)
                _quit_flag.set()
        signal.signal(signal.SIGINT, _sigint)

        active = {k: v for k, v in {**meta_opts, **html_opts}.items() if v}
        _level_name = {L_INFO:'INFO', L_NOTE:'NOTE',
                       L_WARNING:'WARNING', L_VERBOSE:'VERBOSE'}[_log_level]
        _log_status = (f"→ {LOG_FILE}" if _log_file_handle else "off")

        print(f"\n  Found {len(epubs)} EPUB(s).")
        print(f"  Images   : quality={JPEG_QUALITY}, max={MAX_SIZE[0]}px"
              + (" [DISABLED]" if not HAS_PIL else ""))
        print(f"  ZIP      : level {ZIP_LEVEL}  |  TOC depth {TOC_MAX_DEPTH}"
              f"  |  Workers {MAX_WORKERS}")
        print(f"  Log level: {_level_name}  |  Log file: {_log_status}"
              + ("  |  Font subsetting: ON" if html_opts.get('do_subset_fonts') and HAS_FONTTOOLS
                 else ("  |  Font subsetting: OFF"
                       + (" (install fonttools)" if not HAS_FONTTOOLS else "")))
              + ("  |  WOFF2: ON" if html_opts.get('do_convert_woff2') and HAS_BROTLI else ""))
        if active:
            print(f"  Active   : {', '.join(k for k in active)}")
        print()

        work = [
            (f,
             os.path.join(SOURCE_DIR, f),
             os.path.join(OUTPUT_DIR, f),
             {**meta_opts, **html_opts},
             args.verbose)
            for f in epubs
        ]
        skipped, processed, total_saved, total_meta = 0, 0, 0.0, 0
        _n_done = 0

        def _ui_write(msg, level=L_NOTE, is_stderr=False):
            # Thread-safe print, filtered by log level. Log file always receives all levels.
            with _lock:
                if _log_file_handle is not None:
                    level_tag = {L_WARNING: 'WARN', L_INFO: 'INFO',
                                 L_NOTE: 'NOTE', L_VERBOSE: 'VERB'}.get(level, 'NOTE')
                    _log_file_handle.write(
                        f"{datetime.now().strftime('%H:%M:%S')}  [{level_tag}]  {msg.strip()}\n"
                    )
                    _log_file_handle.flush()
                if level > _log_level: return
                out = sys.stderr if is_stderr else sys.stdout
                tqdm.write(msg, file=out)

        def _handle_result(fname, bkb, akb, mc, err, log):
            nonlocal skipped, processed, total_saved, total_meta, _n_done
            _n_done += 1
            for _line in (log or []):
                _s = _line.strip()
                if not _s: continue
                _is_warn = _s.lstrip().startswith("Warning")
                _level = (L_WARNING if _is_warn
                          else (L_NOTE if _s.lstrip().startswith("Meta:") else L_VERBOSE))
                if _is_warn and fname not in _s:
                    _s = _s.replace("Warning:", f"Warning: {fname} —", 1)
                _ui_write(f"  {_s}", level=_level, is_stderr=_is_warn)
            if err:
                _ui_write(f"  Skipped: {fname} — {err}", level=L_INFO, is_stderr=True)
                skipped += 1
            else:
                sav = bkb - akb
                pct = (sav / bkb * 100) if bkb > 0 else 0
                total_saved += sav; total_meta += (mc or 0); processed += 1
                _ui_write(
                    f"  Finished: {fname}: {bkb:.0f}→{akb:.0f} KB ({pct:.1f}%)"
                    + (f" [{mc} meta]" if mc else ""),
                    level=L_INFO)

        bar = tqdm(total=len(epubs), unit="book", dynamic_ncols=True)
        if MAX_WORKERS > 1:
            executor = concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS)
            futures = {executor.submit(_worker, a): a[0] for a in work}
            for future in concurrent.futures.as_completed(futures):
                if _quit_flag.is_set():
                    for f in futures: f.cancel()
                    break
                _handle_result(*future.result())
                bar.update(1)
        else:
            for a in work:
                if _quit_flag.is_set(): break
                _handle_result(*_worker(a))
                bar.update(1)
        bar.close()

        # ── Summary ───────────────────────────────────────────────────────────
        _stopped_early = _quit_flag.is_set() and _n_done < len(work)

        print("\n" + "─" * 60)
        if _stopped_early:
            print(f"\n  Stopped early.  {processed} optimised, {skipped} skipped"
                  f" ({len(work) - _n_done} remaining).")
        else:
            print(f"\n  Done.  {processed} optimised, {skipped} skipped.")
        if processed:
            print(f"  Space saved     : {total_saved / 1024:.1f} MB")
            print(f"  Metadata changes: {total_meta}")
        if _warn_count:
            print(f"  Warnings        : {_warn_count}"
                  + (f"  (see {LOG_FILE})" if _log_file_handle else ""))
        print()

        if _log_file_handle is not None:
            _log_file_handle.write(
                f"\n--- Session ended: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                f"  processed={processed}  skipped={skipped}"
                f"  warnings={_warn_count}"
                + (f"  stopped_early=yes  remaining={len(work) - _n_done}"
                   if _stopped_early else "") + "\n"
            )
            _log_file_handle.flush()
            _log_file_handle.close()


if __name__ == "__main__": main()
