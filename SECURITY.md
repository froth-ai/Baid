# Security policy

## Reporting a vulnerability

Report security issues privately. Do not open a public issue, pull request or discussion.

Use GitHub's private vulnerability reporting: open the repository's **Security** tab and
choose **Report a vulnerability**. Include what you found, how to reproduce it, and the commit
you tested. We will acknowledge the report and keep you updated until it is fixed.

## Tenant isolation bugs are security bugs

GraphCache serves many tenants from one cache. Any way for one tenant to read, infer, poison or
evict another tenant's data counts as a security bug and must be reported privately. Examples:

- a cache hit that returns another tenant's memory items;
- a request that chooses its tenant from the request body instead of the API key;
- one tenant's writes invalidating or overwriting another tenant's entries;
- tenant data appearing in logs, metrics labels or traces visible to another tenant.

## Supported versions

GraphCache is pre-alpha. Only the latest `main` gets fixes.
