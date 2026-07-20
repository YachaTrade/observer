# VaultRegistry (v2 구현)

**EventType**: `VaultRegistry`
**체크포인트**: `vault_registry`
**컨트랙트**: VaultRegistry
**ABI**: `abi/v2/VaultRegistry.json`
**주소 설정**: `VAULT_REGISTRY` (선택)
**블록 의존성**: 없음 (admin-driven, 프로토콜 레벨)

---

## 역할

`VaultRegistry` 는 관리자가 등록한 singleton vault 컨트랙트들의 목록을 저장한다. Observer 는 `Register` / `Deactivate` 이벤트를 인덱싱하고, Register 이벤트 발생 시 해당 vault 의 `metadataURI()` view 함수를 eth_call 로 호출해 off-chain JSON 메타데이터를 가져와 함께 저장한다.

## VaultType enum 매핑

컨트랙트: `nadfun-contract-v2/src/interfaces/IVaultRegistry.sol`

| on-chain 값 | Rust `RegisteredVaultType` | DB `vault_type` |
|---|---|---|
| 0 | `Custom` | `'CUSTOM'` |
| 1 | `Burn` | `'BURN'` |
| 2 | `Lp` | `'LP'` |
| 3 | `Transfer` | `'TRANSFER'` |
| 4 | `Gift` | `'GIFT'` |

신규 VaultType 추가 시 Rust enum + DB CHECK 둘 다 업데이트 필요.

---

## Register (볼트 등록)

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| vault | address (indexed) | 등록되는 vault 컨트랙트 주소 |
| name | string | 사람이 읽을 수 있는 이름 (e.g. "BurnVault") |
| creator | address | 등록 트랜잭션의 `msg.sender` (admin) |
| vaultType | uint8 | `IVaultRegistry.VaultType` enum |

### 부가 동작 (cache-first, token URI 플로우와 동일)

1. Register 이벤트 블록에서 **`vault.metadataURI()` eth_call** 로 canonical URI를 가져온다.
2. `V2VaultRegistryController::fetch_cached_metadata(vault_id)`의 URI가 canonical URI와 같으면 저장된 JSON을 재사용해 HTTP를 건너뛴다.
3. 캐시 miss 또는 URI 변경 시, URI가 정확히 `https://storage.nadapp.net/*.json` 패턴이면 HTTP fetch (redirect 미허용, 응답 최대 1 MiB, 최대 5회 재시도, 300ms backoff) → `VaultMetadata` JSON 파싱.
4. 실패 시 `metadata_uri` 는 저장하되 `metadata = NULL` 로 두고 warn 로그. 나중에 백필 가능.

### Off-chain JSON 스키마

```json
{
  "name": "Buyback & Burn",
  "description": {
    "what": "...",
    "how": ["...", "..."],
    "rules": ["...", "..."],
    "importantNote": "..."
  },
  "imageUri": "https://storage.nadapp.net/vault/image/buyback.png"
}
```

### DB 저장

**`v2_vault_registry`** (append-only 이벤트 로그):
- `vault_id, transaction_hash, block_number, created_at, log_index, tx_index`

**`v2_vault_metadata`** (denormalized, PK=vault_id, upsert):
- `name, creator, vault_type, active=TRUE, metadata_uri, metadata (JSONB), metadata_fetched_at, registered_at, updated_at`
- ON CONFLICT: `registered_at` 비교로 오래된 replay 가 최신 상태를 덮어쓰는 것 방지.

---

## Deactivate (볼트 비활성화)

### 이벤트 필드
| 필드 | 타입 | 설명 |
|------|------|------|
| vault | address (indexed) | 대상 vault |
| active | bool | 새 상태 (true=활성, false=비활성) |

### DB 동작

`v2_vault_metadata.active` 컬럼을 UPDATE. `updated_at` guard 로 reorg replay 시 이전 상태로 돌아가는 것 방지. 별도 히스토리 테이블은 없음.

---

## 쿼리 예시

**활성 볼트 전체 목록**:
```sql
SELECT * FROM v2_vault_metadata WHERE active;
```

**특정 타입 활성 볼트**:
```sql
SELECT * FROM v2_vault_metadata
WHERE active AND vault_type = 'BURN';
```

**메타데이터 JSON 안에서 특정 필드 조회** (JSONB path operator):
```sql
SELECT vault_id, metadata->>'name' AS display_name,
       metadata->'description'->>'what' AS description_what
FROM v2_vault_metadata
WHERE active;
```

**메타데이터 fetch 실패한 볼트 (백필 대상)**:
```sql
SELECT vault_id, metadata_uri
FROM v2_vault_metadata
WHERE metadata IS NULL;
```

---

## 운영 주의

- **URL allowlist**: `https://storage.nadapp.net/*.json` 만 fetch. 다른 도메인은 거부 + warn.
- **Fetch timeout**: 10초. 실패 시 **최대 5회 재시도** (300ms backoff, transport 에러·non-2xx HTTP 에서 재시도; 404 는 즉시 포기). `utils/metadata.rs` (token URI) 와 동일 정책.
- **URI 검증 후 DB 캐시**: replay도 이벤트 블록의 URI는 RPC로 확인하며, URI가 같을 때만 저장된 JSON을 재사용해 HTTP를 생략한다.
- **백필**: observer 초기 sync 시 `from_block` 을 VaultRegistry 배포 블록 이후로 설정하면 과거 Register 이벤트들이 순차 인덱싱됨. 별도 백필 스크립트는 현재 없음. 과거 fetch 실패 건은 `metadata IS NULL` 쿼리로 추출 후 재처리.
- **Registry 주소 미설정**: `VAULT_REGISTRY` env가 비어 있으면 registry 로그를 처리하지 않으며, 나머지 핸들러는 계속 실행된다.
