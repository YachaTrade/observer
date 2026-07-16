use std::sync::atomic::Ordering;

use alloy::eips::BlockNumberOrTag;
use alloy::primitives::TxHash;
use alloy::providers::Provider;
use alloy::pubsub::SubscriptionStream;
use alloy::rpc::types::{Block, Filter, Header, Log, Transaction};
use anyhow::{Context, Result};

use super::RpcClient;

impl RpcClient {
    // 캐시된 최신 블록 번호 가져오기 (즉시 반환)
    pub fn get_cached_latest_block(&self) -> u64 {
        self.latest_block.load(Ordering::Relaxed)
    }

    // 실시간으로 최신 블록 번호 가져오기 (네트워크 요청)
    pub async fn get_latest_block_number(&self) -> Result<u64> {
        crate::measure_rpc!(
            "get_block_number",
            self.execute_with_fallback(|provider| {
                Box::pin(async move {
                    provider
                        .get_block_number()
                        .await
                        .context("Failed to get latest block number")
                })
            })
        )
    }

    async fn get_block_by_number(&self, block_number: u64) -> Result<Option<Block>> {
        crate::measure_rpc!(
            "get_block_by_number",
            self.execute_with_fallback(|provider| {
                Box::pin(async move {
                    provider
                        .get_block_by_number(BlockNumberOrTag::Number(block_number))
                        .await
                        .context(format!("Failed to get block by number {}", block_number))
                })
            })
        )
    }

    pub async fn get_block_timestamp(&self, block_number: u64) -> Result<u64> {
        let block = self.get_block_by_number(block_number).await?;

        match block {
            Some(b) => Ok(b.header.timestamp),
            None => Ok(chrono::Utc::now().timestamp() as u64),
        }
    }

    pub async fn get_logs(&self, filter: Filter) -> Result<Vec<Log>> {
        crate::measure_rpc!(
            "get_logs",
            self.execute_with_fallback(|provider| {
                let filter = filter.clone();
                Box::pin(async move {
                    provider
                        .get_logs(&filter)
                        .await
                        .context("Failed to get logs")
                })
            })
        )
    }

    pub async fn get_transaction_by_hash(&self, hash: TxHash) -> Result<Option<Transaction>> {
        crate::measure_rpc!(
            "get_transaction_by_hash",
            self.execute_with_fallback(|provider| {
                Box::pin(async move {
                    provider
                        .get_transaction_by_hash(hash)
                        .await
                        .context("Failed to get transaction by hash")
                })
            })
        )
    }

    pub async fn get_transaction_receipt(
        &self,
        hash: TxHash,
    ) -> Result<Option<alloy::rpc::types::TransactionReceipt>> {
        crate::measure_rpc!(
            "get_transaction_receipt",
            self.execute_with_fallback(|provider| {
                Box::pin(async move {
                    provider
                        .get_transaction_receipt(hash)
                        .await
                        .context("Failed to get transaction receipt")
                })
            })
        )
    }

    /// Get native balance at a specific block
    pub async fn get_native_balance_at_block(
        &self,
        address: alloy::primitives::Address,
        block_number: u64,
    ) -> Result<alloy::primitives::U256> {
        crate::measure_rpc!(
            "get_balance",
            self.execute_with_fallback(|provider| {
                Box::pin(async move {
                    provider
                        .get_balance(address)
                        .block_id(BlockNumberOrTag::Number(block_number).into())
                        .await
                        .context("Failed to get native balance")
                })
            })
        )
    }

    /// Get code at address (empty = EOA, non-empty = Contract)
    pub async fn get_code(
        &self,
        address: alloy::primitives::Address,
    ) -> Result<alloy::primitives::Bytes> {
        crate::measure_rpc!(
            "get_code",
            self.execute_with_fallback(|provider| {
                Box::pin(async move {
                    provider
                        .get_code_at(address)
                        .await
                        .context("Failed to get code")
                })
            })
        )
    }

    // Contract call method for balance queries
    pub async fn call_contract<T>(
        &self,
        call: T,
        to: alloy::primitives::Address,
    ) -> Result<T::Return>
    where
        T: alloy::sol_types::SolCall + Clone + Send + Sync + 'static,
        T::Return: Send + Sync + 'static,
    {
        use alloy::primitives::Bytes;
        use alloy::rpc::types::TransactionRequest;
        use std::time::Duration;

        use crate::config::RPC_TIME_OUT;

        self.execute_with_fallback(|provider| {
            let call = call.clone();
            Box::pin(async move {
                let calldata: Bytes = call.abi_encode().into();
                let tx = TransactionRequest::default().to(to).input(calldata.into());

                let result =
                    tokio::time::timeout(Duration::from_millis(*RPC_TIME_OUT), provider.call(tx))
                        .await
                        .map_err(|_| anyhow::anyhow!("Contract call timed out"))?
                        .context("Failed to call contract")?;

                T::abi_decode_returns(&result)
                    .map_err(|e| anyhow::anyhow!("Failed to decode return data: {}", e))
            })
        })
        .await
    }

    // Contract call at specific block number (with fallback)
    pub async fn call_contract_at_block<T>(
        &self,
        call: T,
        to: alloy::primitives::Address,
        block_number: u64,
    ) -> Result<T::Return>
    where
        T: alloy::sol_types::SolCall + Clone + Send + Sync + 'static,
        T::Return: Send + Sync + 'static,
    {
        use alloy::primitives::Bytes;
        use alloy::rpc::types::TransactionRequest;
        use std::time::Duration;

        use crate::config::RPC_TIME_OUT;

        self.execute_with_fallback(|provider| {
            let call = call.clone();
            Box::pin(async move {
                let calldata: Bytes = call.abi_encode().into();
                let tx = TransactionRequest::default().to(to).input(calldata.into());

                let result = tokio::time::timeout(
                    Duration::from_millis(*RPC_TIME_OUT),
                    provider
                        .call(tx)
                        .block(BlockNumberOrTag::Number(block_number).into()),
                )
                .await
                .map_err(|_| anyhow::anyhow!("Contract call timed out"))?
                .context("Failed to call contract")?;

                T::abi_decode_returns(&result)
                    .map_err(|e| anyhow::anyhow!("Failed to decode return data: {}", e))
            })
        })
        .await
    }

    pub async fn get_stream(&self) -> Result<SubscriptionStream<Header>> {
        let provider = self.get_current_provider().await?;
        let subscription = provider
            .subscribe_blocks()
            .await
            .context("Failed to subscribe to blocks")?;
        let stream = subscription.into_stream();
        Ok(stream)
    }

    // 백그라운드 업데이터 중지
    pub async fn stop_block_updater(&mut self) {
        use tracing::info;

        if let Some(handle) = self.block_updater_handle.take() {
            handle.abort();
            info!("Stopped background block updater");
        }
    }

    // 블록 업데이터 상태 확인
    pub fn is_block_updater_running(&self) -> bool {
        self.block_updater_handle
            .as_ref()
            .map(|h| !h.is_finished())
            .unwrap_or(false)
    }
}
