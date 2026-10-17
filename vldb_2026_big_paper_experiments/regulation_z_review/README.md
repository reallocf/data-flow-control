# Local policy provenance reviewer

Open `index.html` in a browser. It works directly from disk, without a server,
network connection, installation, or publication. Keep index.html, style.css,
app.js and data.js together. Only the optional CFPB links require the internet.

1. Select one of 31 checks in the left column.
2. Blue cards show direct motivation; amber cards show shared support. Select a
   proposition to display its source paragraph(s), highlighted phrases, and limits.
3. Expand the predicate/computation disclosures to inspect the implementation.
4. Use “Show all 40” to inspect supporting propositions; reverse links appear
   below the source. The URL fragment preserves the chosen policy/proposition.

Cxx is a display alias, not a new policy ID in the implementation. Numbers match
proposition IDs and therefore have gaps. The 40 proposition paraphrases and the
shared dependency links are curated analysis. They are not legal source quotations
or a mechanically proven dependency graph. No new regulatory inventory is claimed.

Source HTML was retrieved from CFPB on 2026-10-14 and retained in `sources/`.
The viewer extracts paragraph text and normalizes whitespace without changing
wording; it never embeds or runs scripts from the downloaded pages. P24 includes
selected external definition/commentary paragraphs, not the advertising section's
unrelated requirements. Source URLs, page version labels, and SHA-256 hashes are
visible in the information disclosure. Code hashes identify the implementation
snapshot, which will not automatically change when the repository changes.

To refresh after code edits, from the experiment project directory:

```sh
.venv/bin/python regulation_z_review/build_data.py
```

The builder uses the saved source HTML, validates all paragraph references and
highlight phrases, checks 31/40 counts and that every proposition has an edge,
and writes data.js plus the downloadable data.json. New source retrieval requires
updating the saved HTML and its recorded retrieval date deliberately.

For a localhost preview:

```sh
python3 -m http.server 8767 --bind 127.0.0.1 --directory regulation_z_review
```

Open http://127.0.0.1:8767. No policies or experiment files are modified by the viewer.
