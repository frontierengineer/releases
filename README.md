# Frontier releases

Release channel manifests and release notes for [Frontier](https://frontierengineer.com).

- `channels/<channel>.json` — the current build for each channel (`nightly`, `rc`, `stable`).
  Every running Frontier install polls its channel's manifest to discover updates;
  the internal environment updaters and the future desktop app read the same files.
- `notes/` — release notes, one file per version.

Published automatically by the release pipeline in `frontierengineer/frontier`.
Served via GitHub Pages; `releases.frontierengineer.com` will point here.
