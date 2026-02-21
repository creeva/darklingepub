📚 Darkling EPUB Toolkit

A deterministic, safe, and highly optimized Python utility for cleaning, repairing, and standardizing EPUB files.

The Darkling EPUB Toolkit is designed to be the ultimate batch-processing tool for your digital library. It acts as an EPUB Stabilizer and Optimizer, fixing mangled metadata, repairing poorly formatted HTML/CSS, reducing file sizes through safe image optimization, and enforcing strict e-reader compatibility standards—all without risking file corruption.

🚀 Key Features
📖 Metadata Cleaning (XML-Aware & Non-Destructive)

Titles: Corrects all-caps or all-lowercase titles. Strips annoying retail artifacts (e.g., (Retail), [Kindle], – A Novel) and regenerates proper opf:file-as sort keys by moving leading articles to the end.

Authors: Fixes casing and inverted "Last, First" display names. Standardizes opf:role using 3-letter MARC codes (defaulting to aut) and demotes secondary authors to contributors instead of deleting them.

Publishers: Converts all-caps publisher names to Title Case. Strips out URLs, legal suffixes (Inc., LLC), verbose phrasing (Publishing Group), and consolidates known imprints to their parent companies (e.g., Knopf → Penguin Random House).

Subjects & Tags: Removes blocklisted "noise" tags (Fiction, General, Ebook), drops multi-word non-hyphenated subjects, and deduplicates the rest.

Dates & Languages: Normalizes all malformed dates into strict ISO 8601 format (e.g., 2023-01-15) and language codes to BCP 47 (e.g., en-US).

Clean Identifiers: Ensures only one ISBN is kept per scheme and aggressively removes empty or duplicate metadata elements.

🧹 HTML, CSS, and Structural Polish

Dark Mode Compatibility: Strips hardcoded background-color rules from the body and html CSS tags to prevent text readability issues on modern e-readers.

Deprecated Attribute Removal: Scrubs outdated HTML attributes like align=, bgcolor=, border=, and valign=.

Print Artifact Scrubbing: Removes irrelevant text leftover from physical prints (e.g., "acid-free paper" or printer's keys).

TOC/NAV Generation: Automatically generates a missing EPUB3 NAV document from the existing NCX file (or safely falls back to a flat spine-based NAV if the NCX is invalid).

Typography Normalization: Detects and forces UTF-8 text encoding to fix "mojibake" (garbled text), while standardizing smart quotes, dashes, and ellipses.

🗜️ Archive & Image Optimization (File Size Reduction)

Image Resizing: Proportionally resizes excessively large images to strict maximum bounds without upscaling.

Image Compression & Cleanup: Re-saves JPEGs conservatively, strips all bloated EXIF metadata, converts CMYK JPEGs to RGB for e-reader compatibility, and preserves PNGs natively.

Strict Archive Integrity: Re-packages the EPUB ZIP archive to enforce specification rules, ensuring the mimetype file is written first and completely uncompressed.

Junk Removal: Scours the archive for OS-level junk (.DS_Store, thumbs.db), unused images, and duplicate spine items, safely deleting them.

🛠️ Installation & Requirements

Ensure you have Python 3.7+ installed. The script relies on standard text processing and imaging libraries.

Clone the repository or download epub_toolkit.py.

Install the required Python dependencies:

💻 Command-Line Usage
The tool is built for both single-file fixes and massive batch processing. It uses multiprocessing to chew through large directories quickly.

Basic Syntax:

Process a single EPUB:

Process a whole directory of EPUBs:

Supported Arguments
(Note: Run python epub_toolkit.py -h or --help to see the exact flags configured in your version.)

path (Required): The file or directory you want to process.

-w, --workers (Optional): Set the number of concurrent CPU workers for batch processing. Defaults to your system's maximum available cores minus one.

--salvage (Optional/If Implemented): Bypasses strict deterministic safety to run heuristic, high-risk repairs (like guessing table of contents or rebuilding dead OPF files). Not recommended for bulk processing.

⚙️ Internal Configuration Variables
If you open epub_toolkit.py in a text editor, you will find a configuration block at the top of the script. You can tweak these internal variables to customize the script's behavior to your specific library preferences:

MAX_SUBJECTS = 5
Controls the maximum number of genre/subject tags kept in the metadata. Any tags beyond this number (after junk tags are filtered out) are discarded. Set to 0 or None to keep all valid tags.

JPEG_QUALITY = 70
The compression floor used when re-saving JPEG images. 70 offers an excellent balance of massive file-size reduction while retaining acceptable visual quality for e-ink and mobile screens.

MAX_COVER_DIM = 2560
The maximum pixel height or width for the book's cover image. Images exceeding this will be proportionally scaled down.

MAX_IMAGE_DIM = 1600
The maximum pixel height or width for internal book illustrations/images.

NOISE_TAGS / BLOCKLIST (List/Set)
A customizable list of words the script will automatically delete from the metadata tags (e.g., "Fiction", "Ebook", "Amazon"). Add your own annoyances to this list.

PUBLISHER_CONSOLIDATION (Dictionary)
A mapping used to clean up messy imprints. For example, {"Knopf": "Penguin Random House", "Tor": "Macmillan"}. You can expand this dictionary to automatically group your library by parent publishers.

🛡️ Safety & Philosophy
This script is built on the philosophy of being a Deterministic Stabilizer.

It will never guess missing structural components.

It will never fabricate files.

If a file cannot be parsed safely, the script catches the error, logs a warning, and skips the file rather than corrupting your book.
