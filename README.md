# Frontier releases

Frontier's published builds, for anyone to download with no sign-up. The
release pipeline in [frontierengineer/frontier](https://github.com/frontierengineer/frontier)
writes everything here as GitHub Releases; nothing in this repository's files is read.

- **`build-<build id>`**: one release per build cut to release candidate. It holds the
  build's files for each platform, `build-manifest.json` (signed), `notes.md` and the
  Application Interface the build offers.
- **`channel-release-candidate`** and **`channel-stable`**: each holds the channel's
  signed `channel-manifest.json`, naming the build the channel offers and every build it
  has published, and the channel's notes. Every installation reads its channel's manifest
  and checks it with the key built into Frontier before it offers an update.
- The release GitHub marks **Latest** is the build stable names, so
  `releases/latest/download/<file>` always serves stable's packages.

`notes/` keeps the proof of concept's release notes as raw material; nothing produces or
reads them.
