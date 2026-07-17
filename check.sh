#!/bin/bash

# GIWA RPC (RPC_URL env로 재정의 가능)
RPC_URL="${RPC_URL:-https://sepolia-rpc.giwa.io}"

# Transfer 이벤트 해시
TRANSFER_TOPIC="0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# 색상 설정
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 로그 출력 함수
log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# 최신 블록 번호 조회 함수
get_latest_block() {
    local response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        -d '{
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 1
        }' \
        "$RPC_URL")
    
    # 디버깅: 응답 확인
    if [ -z "$response" ]; then
        log_error "RPC 응답이 비어있음"
        return 1
    fi
    
    # 에러 응답 확인
    if echo "$response" | grep -q '"error"'; then
        log_error "RPC 에러 응답: $response"
        return 1
    fi
    
    # result 필드 추출
    local hex_result=$(echo "$response" | grep -o '"result":"0x[0-9a-fA-F]*"' | cut -d'"' -f4)
    
    if [ -z "$hex_result" ]; then
        log_error "블록 번호 추출 실패. 응답: $response"
        return 1
    fi
    
    # 16진수를 10진수로 변환
    local decimal_result=$(printf "%d" "$hex_result" 2>/dev/null)
    
    if [ -z "$decimal_result" ] || [ "$decimal_result" = "0" ]; then
        log_error "블록 번호 변환 실패. hex: $hex_result"
        return 1
    fi
    
    echo "$decimal_result"
}

# 로그 조회 함수
get_transfer_logs() {
    local from_block=$1
    local to_block=$2
    
    local response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        -d '{
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [{
                "fromBlock": "0x'$(printf "%x" $from_block)'",
                "toBlock": "0x'$(printf "%x" $to_block)'",
                "topics": ["'$TRANSFER_TOPIC'"]
            }],
            "id": 1
        }' \
        "$RPC_URL")
    
    # 로그 개수 반환 (result 배열의 길이)
    echo "$response" | grep -o '"result":\[.*\]' | sed 's/"result":\[//' | sed 's/\]$//' | awk -F'},' '{print NF}' | head -1
}

# 메인 실행부
main() {
    log_info "Transfer 이벤트 모니터링 시작"
    log_info "RPC URL: $RPC_URL"
    log_info "Transfer Topic: $TRANSFER_TOPIC"
    echo ""
    
    # 블록 범위 설정 (한 번만)
    log_info "기준 블록 조회 중..."
    BASE_BLOCK=$(get_latest_block)
    local base_status=$?
    
    if [ $base_status -ne 0 ] || [ -z "$BASE_BLOCK" ]; then
        log_error "기준 블록 조회 실패"
        exit 1
    fi
    
    # FROM과 TO 블록을 기준 블록 + 2, + 4로 설정
    FROM_BLOCK=$((BASE_BLOCK + 1))
    TO_BLOCK=$((BASE_BLOCK + 1))
    
    log_success "모니터링 블록 범위 설정: $FROM_BLOCK - $TO_BLOCK (기준: $BASE_BLOCK)"
    echo ""
    
    # 무한 루프로 해당 범위 모니터링
    while true; do
        # 동일한 범위의 Transfer 로그 조회
        LOG_COUNT=$(get_transfer_logs $FROM_BLOCK $TO_BLOCK)
        
        # 결과가 비어있거나 숫자가 아닌 경우 0으로 처리
        if [ -z "$LOG_COUNT" ] || ! [[ "$LOG_COUNT" =~ ^[0-9]+$ ]]; then
            LOG_COUNT=0
        fi
        
        # 매번 결과 출력
        if [ "$LOG_COUNT" -gt 0 ]; then
            log_success "블록 범위: $FROM_BLOCK - $TO_BLOCK | Transfer 이벤트: $LOG_COUNT 개"
        else
            log_info "블록 범위: $FROM_BLOCK - $TO_BLOCK | Transfer 이벤트: $LOG_COUNT 개"
        fi
        
        # 1ms 대기
        sleep 0.001
    done
}

# Ctrl+C 처리
trap 'log_warning "모니터링 중단됨"; exit 0' INT

# 스크립트 실행
main
