# ADR-009: Fail open on any internal error

## Context

GraphCache can never make an agent less available than its backend.

## Decision

Fail open on any internal error.

## Consequences

Trade-off accepted: A bug can silently remove the speed-up; needs alerting.

## Status

Accepted

Source: architecture.md section 13. Change this decision only with a new ADR.
