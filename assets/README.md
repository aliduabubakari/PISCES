# Project Assets

- `brand/pisces-mark.svg`: canonical, transparent, scalable pipeline-shaped P mark.
- `brand/pisces-wordmark.svg`: README/documentation lockup; text has system-font fallbacks.
- `screenshots/`: deliberately curated screenshots of the real application using
  the committed, sanitized synthetic examples. These are illustrative saved
  observations, not new benchmark results or measurement attestations.

The mark is a project-native vector asset, not an external icon or stock image.
Its three nodes suggest pipeline stages; they do not imply task measurements or
a particular execution graph. Teal `#0C7164`, blue `#477A9C`, charcoal `#202B29`.
Assets are covered by the repository's license. Retain meaningful alternative
text when embedding screenshots and keep original SVGs as the editable source.

The frontend imports the mark directly from this folder and uses the same SVG
as its favicon. No external image host or font service is needed by the app.
Raw diagnostics, screenshots taken during tests and generated reports belong in
ignored output directories, not this curated folder.

## Refresh Screenshots

With frontend dependencies installed and Vite running on `127.0.0.1:5173`, run
`node scripts/capture_readme_screenshots.mjs` from the repository root (Node 22.12+
and an installed Chrome browser; `PISCES_BROWSER_CHANNEL` can select another
installed Playwright channel). The script only opens committed public examples
and a local script draft: it does not connect, submit a job or read a token.
Review the images before committing them. They include overview, task inspection,
load campaign and new evaluation views; hidden values are not reconstructed.
