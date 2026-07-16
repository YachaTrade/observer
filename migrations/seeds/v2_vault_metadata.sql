-- Backfill v2_vault_metadata for the 4 singleton V2 vaults.
-- Generated from on-chain getVaultInfo + metadataURI calls (testnet).
-- registered_at / updated_at / metadata_fetched_at are stamped now
-- because Register events are too far in the past to recover.
--
-- IVaultRegistry.VaultType: 0=Custom 1=Burn 2=Lp 3=CreatorFee 4=Gift
-- GiftVault was registered on-chain with VaultType=3 (CreatorFee) by
-- mistake. We store the intended 'GIFT' here.

BEGIN;

-- BurnVault (0xB1c3574e2E7Ca6Dd1b9E2BB833EcaB2b33d95C17)
INSERT INTO v2_vault_metadata
    (vault_id, name, creator, vault_type, active,
     metadata_uri, metadata, metadata_fetched_at,
     registered_at, updated_at)
VALUES (
    '0xB1c3574e2E7Ca6Dd1b9E2BB833EcaB2b33d95C17',
    'BurnVault',
    '0x119af7e51A606510745af94284c6B398d4d2836a',
    'BURN',
    TRUE,
    'https://storage.nadapp.net/vault/metadata/buyback.json',
    $vault${
  "name": "Buyback & Burn",
  "description": {
    "what": "Buyback & Burn uses a portion of trading fees to buy back the token from the market and permanently remove it from circulation. It is designed to reduce supply and support the token's long-term structure.",
    "how": [
      "When trades occur, the allocated portion of fees is accumulated.",
      "Once a preset threshold is reached, the accumulated fees are automatically used to buy back the token from the market.",
      "The purchased tokens are then permanently burned and cannot re-enter circulation."
    ],
    "rules": [
      "The allocated fees are used only for this token's buyback and burn process.",
      "Purchased tokens are burned immediately and are not sent to any other wallet.",
      "No manual claim or separate user action is required."
    ],
    "importantNote": "Buyback & Burn is designed to reduce circulating supply and support the token's overall structure, but it does not guarantee price increases or investment returns."
  },
  "imageUri": "https://storage.nadapp.net/vault/image/buyback.png"
}
$vault$::jsonb,
    EXTRACT(EPOCH FROM NOW())::BIGINT,
    EXTRACT(EPOCH FROM NOW())::BIGINT,
    EXTRACT(EPOCH FROM NOW())::BIGINT
)
ON CONFLICT (vault_id) DO UPDATE SET
    name = EXCLUDED.name,
    creator = EXCLUDED.creator,
    vault_type = EXCLUDED.vault_type,
    active = EXCLUDED.active,
    metadata_uri = EXCLUDED.metadata_uri,
    metadata = EXCLUDED.metadata,
    metadata_fetched_at = EXCLUDED.metadata_fetched_at,
    updated_at = EXCLUDED.updated_at;

-- LPVault (0xd5882161162be2E7C34b87a9bCf6b65551718F36)
INSERT INTO v2_vault_metadata
    (vault_id, name, creator, vault_type, active,
     metadata_uri, metadata, metadata_fetched_at,
     registered_at, updated_at)
VALUES (
    '0xd5882161162be2E7C34b87a9bCf6b65551718F36',
    'LPVault',
    '0x119af7e51A606510745af94284c6B398d4d2836a',
    'LP',
    TRUE,
    'https://storage.nadapp.net/vault/metadata/lp.json',
    $vault${
  "name": "LP Support",
  "description": {
    "what": "LP Support uses a portion of trading fees to support the protocol's designated liquidity pool. It is designed to help strengthen liquidity and support a more stable trading environment.",
    "how": [
      "When trades occur, the allocated portion of fees is accumulated.",
      "Once a preset threshold is reached, the accumulated fees are automatically used to support the designated liquidity pool.",
      "This helps improve liquidity depth and supports a more stable trading environment."
    ],
    "rules": [
      "The allocated fees are used only for the protocol's designated liquidity pool.",
      "No manual claim or separate user action is required.",
      "This is not a mechanism for directly distributing rewards to LP providers."
    ],
    "importantNote": "LP Support is designed to strengthen liquidity and support more stable trading conditions, but it does not guarantee profits or price increases."
  },
  "imageUri": "https://storage.nadapp.net/vault/image/lp.png"
}
$vault$::jsonb,
    EXTRACT(EPOCH FROM NOW())::BIGINT,
    EXTRACT(EPOCH FROM NOW())::BIGINT,
    EXTRACT(EPOCH FROM NOW())::BIGINT
)
ON CONFLICT (vault_id) DO UPDATE SET
    name = EXCLUDED.name,
    creator = EXCLUDED.creator,
    vault_type = EXCLUDED.vault_type,
    active = EXCLUDED.active,
    metadata_uri = EXCLUDED.metadata_uri,
    metadata = EXCLUDED.metadata,
    metadata_fetched_at = EXCLUDED.metadata_fetched_at,
    updated_at = EXCLUDED.updated_at;

-- CreatorFeeVault (0xFD8f6284504C9e7Aa46fB3F71827B5552F01936d)
INSERT INTO v2_vault_metadata
    (vault_id, name, creator, vault_type, active,
     metadata_uri, metadata, metadata_fetched_at,
     registered_at, updated_at)
VALUES (
    '0xFD8f6284504C9e7Aa46fB3F71827B5552F01936d',
    'CreatorFeeVault',
    '0x119af7e51A606510745af94284c6B398d4d2836a',
    'CREATOR_FEE',
    TRUE,
    'https://storage.nadapp.net/vault/metadata/creator.json',
    $vault${
  "name": "Creator",
  "description": {
    "what": "Creator allocates a portion of trading fees to the wallet linked at token creation. It is designed to help support the creator's ongoing project operations.",
    "how": [
      "When trades occur, the allocated portion of fees is accumulated.",
      "Once a preset threshold is reached, the accumulated fees are automatically paid out to the wallet linked at token creation.",
      "The creator can use these funds to support ongoing project operations."
    ],
    "rules": [
      "Fees are sent only to the wallet linked at the time of token creation.",
      "The recipient wallet address cannot be changed after the token is created.",
      "No manual claim or separate user action is required."
    ],
    "importantNote": "The wallet connected at token creation becomes the fixed wallet for Creator fees, so make sure it is the correct wallet for project operations."
  },
  "imageUri": "https://storage.nadapp.net/vault/image/creator.png"
}
$vault$::jsonb,
    EXTRACT(EPOCH FROM NOW())::BIGINT,
    EXTRACT(EPOCH FROM NOW())::BIGINT,
    EXTRACT(EPOCH FROM NOW())::BIGINT
)
ON CONFLICT (vault_id) DO UPDATE SET
    name = EXCLUDED.name,
    creator = EXCLUDED.creator,
    vault_type = EXCLUDED.vault_type,
    active = EXCLUDED.active,
    metadata_uri = EXCLUDED.metadata_uri,
    metadata = EXCLUDED.metadata,
    metadata_fetched_at = EXCLUDED.metadata_fetched_at,
    updated_at = EXCLUDED.updated_at;

-- GiftVault (0xd8F03855449Cc508A1B4442c072b1e1e3B064621)
INSERT INTO v2_vault_metadata
    (vault_id, name, creator, vault_type, active,
     metadata_uri, metadata, metadata_fetched_at,
     registered_at, updated_at)
VALUES (
    '0xd8F03855449Cc508A1B4442c072b1e1e3B064621',
    'GiftVault',
    '0x119af7e51A606510745af94284c6B398d4d2836a',
    'GIFT',
    TRUE,
    'https://storage.nadapp.net/vault/metadata/gift.json',
    $vault${
  "name": "Gift",
  "description": {
    "what": "Gift allocates a portion of trading fees to the owner of a designated X account. Once ownership is verified, the fees can be received through a selected EVM wallet address.",
    "how": [
      "At token creation, an X handle is assigned as the fee recipient for Gift.",
      "The owner of that X account verifies ownership by posting the required verification tweet: \"Gift the fees from [Token CA] to [Recipient Wallet Address] #Nadfun\"",
      "Once verification is completed and a preset threshold is reached, the accumulated fees are automatically paid out to the verified EVM wallet address."
    ],
    "rules": [
      "The X handle cannot be changed after token creation.",
      "The fee recipient wallet address cannot be changed once it is set.",
      "The X account owner can complete verification and wallet setup without connecting to nad.fun.",
      "If verification or wallet setup is not completed within 10 days, the allocated fees are redirected to Buyback & Burn."
    ],
    "importantNote": "Gift is controlled by ownership of the designated X account, not by the creator's wallet, and unverified or inactive allocations will be redirected to Buyback & Burn."
  },
  "imageUri": "https://storage.nadapp.net/vault/image/gift.png"
}
$vault$::jsonb,
    EXTRACT(EPOCH FROM NOW())::BIGINT,
    EXTRACT(EPOCH FROM NOW())::BIGINT,
    EXTRACT(EPOCH FROM NOW())::BIGINT
)
ON CONFLICT (vault_id) DO UPDATE SET
    name = EXCLUDED.name,
    creator = EXCLUDED.creator,
    vault_type = EXCLUDED.vault_type,
    active = EXCLUDED.active,
    metadata_uri = EXCLUDED.metadata_uri,
    metadata = EXCLUDED.metadata,
    metadata_fetched_at = EXCLUDED.metadata_fetched_at,
    updated_at = EXCLUDED.updated_at;

COMMIT;
