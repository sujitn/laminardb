# F046: Admin REST API

## Metadata
| Field | Value |
|-------|-------|
| **ID** | F046 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 5 |
| **Dependencies** | F035 |

## Summary
REST API for managing LaminarDB instances, queries, and configuration.

## Endpoints
- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics
- `POST /queries` - Execute SQL
- `GET /jobs` - List running jobs
- `DELETE /jobs/:id` - Cancel job

## Completion Checklist
- [ ] Core endpoints working
- [ ] Authentication integrated
- [ ] OpenAPI spec generated
