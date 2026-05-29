# Docker Image Tagging Convention

## Core (`ghcr.io/openabdev/openab`)

| Tag | Points to | Updated when |
|-----|-----------|--------------|
| `0.8.3-beta.12` | Exact pre-release build | Pre-release tag pushed |
| `beta` | Latest pre-release | Every pre-release build |
| `0.8.3` | Promoted stable build | Stable tag pushed |
| `0.8` | Latest patch in minor | Stable promotion |
| `stable` | Latest stable | Stable promotion |
| `latest` | Latest stable (= `stable`) | Stable promotion |

Variant images (e.g. `-codex`, `-claude`, `-gemini`) follow the same convention with a suffix: `ghcr.io/openabdev/openab-codex:beta`.

## Gateway (`ghcr.io/openabdev/openab-gateway`)

| Tag | Points to | Updated when |
|-----|-----------|--------------|
| `0.5.1` | Exact release | `gateway-v*` tag pushed |
| `v0.5.1` | Same as above (v-prefixed alias) | Same |
| `latest` | Latest release | Every release |

## Which tag to use

| Use case | Recommended tag |
|----------|----------------|
| Production (pinned) | Exact version (`0.8.3-beta.12`) |
| Helm chart default | `stable` or `beta` (channel-based) |
| Local dev / quick test | `beta` |
| CI | Exact version or SHA |

## Release flow

```
release PR merged → tag-on-merge → v0.8.3-beta.12
                                         │
                                         ▼
                                  build-operator.yml
                                         │
                              ┌──────────┴──────────┐
                              │ is_prerelease=true   │
                              ▼                      │
                    tag: 0.8.3-beta.12               │
                    tag: beta                        │
                                                    │
                              ┌──────────────────────┘
                              │ is_prerelease=false (stable)
                              ▼
                    promote latest beta image →
                    tag: 0.8.3
                    tag: 0.8
                    tag: stable
                    tag: latest
```
