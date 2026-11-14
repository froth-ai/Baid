# ADR-008: Tenant comes from the API key, never the request body

## Context

Removes a whole class of isolation bugs.

## Decision

Tenant comes from the API key, never the request body.

## Consequences

Trade-off accepted: Multi-tenant callers need one key per tenant.

## Status

Accepted

Source: architecture.md section 13. Change this decision only with a new ADR.
