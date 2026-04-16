# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in VaultFS, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, email: **adamaniasse153@gmail.com**

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

## Response Timeline

- **Acknowledgment**: within 48 hours
- **Initial assessment**: within 1 week
- **Fix release**: within 2 weeks for critical issues

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

## Security Measures

VaultFS implements:
- API key authentication with SHA256 hashing (keys are never stored in plaintext)
- Path traversal protection
- Input validation on bucket names and object keys
- Security response headers (HSTS, CSP, X-Frame-Options)
- Rate limiting
- Audit logging of all write operations
- HMAC-SHA256 signed presigned URLs and webhooks
- TLS support via rustls
