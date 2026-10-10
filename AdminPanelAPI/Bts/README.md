# BTS (behind-the-scenes media spaces)

Customer file spaces for behind-the-scenes images and videos. Lives in AdminPanelAPI for now and is self-contained so it can move into its own API later: everything is under `Bts/`, wired up by `builder.AddBts()` / `app.UseBts()` in `Program.cs`.

- Pages: a separate static site in [shotdeck/BTS-Filedrop](https://github.com/shotdeck/BTS-Filedrop): `bts/admin.html` (admins) and `bts/space.html#bts_<token>` (customer link, no login). The API doesn't serve them.
- Routes: `/api/bts/admin/...` (signed admin session) and `/api/bts/space/...` (space link token as `Authorization: Bearer`). These do not use the global CORS policy.
- Storage: R2 bucket `bts`, objects under `spaces/{spaceId}/`, deletes move to `trash/{spaceId}/`. Browsers upload/download with presigned URLs.
- Database: `bts` schema (`spaces`, `activity`, `notes`) via `ConnectionStrings:Default`. Run `migrations/052_bts_schema.sql` by hand.

## Configuration

| Key | |
| --- | --- |
| `Bts:R2:AccountId`, `Bts:R2:AccessKey`, `Bts:R2:SecretKey` | R2 token scoped to the `bts` bucket |
| `Bts:R2:BucketName` | default `bts` |
| `Bts:R2:ServiceUrl` | optional, for a local S3 stand-in |
| `Bts:AdminTokenKey` | base64, 32+ bytes; signs admin sessions |
| `Bts:LinkKey` | base64, 32 bytes; seals link tokens so admins can copy them again. Changing it breaks "Share link" for existing spaces (rotate to fix) |
| `Bts:AdminPassword` | optional shared admin password, in addition to the existing admin roster |
| `Bts:PublicSiteUrl` | origin of the BTS-Filedrop site, used in customer links (`<site>/bts/space.html#...`). Required |
| `Bts:MaxFileBytes` | per-file limit, default 50 GB |
| `Bts:AllowedOrigins` | comma-separated origins allowed to call `/api/bts` cross-origin (the hosted static site). BTS routes don't use the API-wide allow-any-origin policy |

## Pages

The pages live in [shotdeck/BTS-Filedrop](https://github.com/shotdeck/BTS-Filedrop) and call this API cross-origin: `window.BTS_API_BASE` in its `bts/js/config.js` is the API origin. Add the site's origin to `Bts:AllowedOrigins` and set `Bts:PublicSiteUrl` to it.

The bucket needs a CORS rule allowing `PUT`/`GET`/`HEAD` from the site's origin with `ETag` exposed, and lifecycle rules to purge `trash/` after 30 days and abort incomplete multipart uploads after 7 days.

Tests: `dotnet test AdminPanelAPI.Tests` (path rules, tokens).

## Zip downloads (admins only)

Admins can zip a whole space (**Download all** on the spaces list or the space header), the current folder, or ticked files and folders. The page asks `POST /api/bts/admin/spaces/{id}/zip-ticket` for a 5-minute signed ticket naming exactly what to include, then submits it as a plain form to `POST /api/bts/zip`. The API streams the objects from R2 straight into the zip (stored, not recompressed), so nothing is buffered in memory or on disk. Each zip download is written to the space's activity log. The form POST is a navigation, so it needs no CORS; an invalid or expired ticket gets `204` and the page stays put.
