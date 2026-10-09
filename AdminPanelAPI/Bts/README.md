# BTS (behind-the-scenes media spaces)

Customer file spaces for behind-the-scenes images and videos. Lives in AdminPanelAPI for now and is self-contained so it can move into its own API later: everything is under `Bts/` and `wwwroot/bts/`, wired up by `builder.AddBts()` / `app.UseBts()` in `Program.cs`.

- Pages: `/bts/admin.html` (admins) and `/bts/space.html#bts_<token>` (customer link, no login).
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
| `Bts:PublicSiteUrl` | origin used in customer links, e.g. `https://adminpanelapi...` |
| `Bts:MaxFileBytes` | per-file limit, default 50 GB |
| `Bts:AllowedOrigins` | comma-separated origins allowed to call `/api/bts` cross-origin (the hosted static site). BTS routes don't use the API-wide allow-any-origin policy |

## Hosting the pages separately

`wwwroot/bts/` also works as a static site: copy it to `<site>/bts/`, set `window.BTS_API_BASE` in `bts/js/config.js` to the API origin, add the site origin to `Bts:AllowedOrigins`, and set `Bts:PublicSiteUrl` to the site so customer links point at `<site>/bts/space.html#...`.

The bucket needs a CORS rule allowing `PUT`/`GET` from the origin(s) serving the pages with `ETag` exposed, and lifecycle rules to purge `trash/` after 30 days and abort incomplete multipart uploads after 7 days.

Tests: `dotnet test AdminPanelAPI.Tests` (path rules, tokens).
