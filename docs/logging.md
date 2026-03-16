# Hệ Thống Logging - Enfyra Server

Enfyra Server cung cấp hệ thống logging toàn diện với file rotation, unique IDs, và correlation tracking để dễ dàng debug và trace requests.

---

## Mục Lục

1. [Tổng Quan](#tổng-quan)
2. [Cấu Trúc Log Files](#cấu-trúc-log-files)
3. [Log Entry Format](#log-entry-format)
4. [API Endpoints](#api-endpoints)
5. [Ví Dụ Sử Dụng](#ví-dụ-sử-dụng)
6. [Trace Request](#trace-request)
7. [Environment Variables](#environment-variables)
8. [Best Practices](#best-practices)

---

## Tổng Quan

### Tính Năng Chính

| Tính năng | Mô tả |
|-----------|-------|
| **Unique Log ID** | Mỗi log entry có ID duy nhất để tìm kiếm nhanh |
| **Correlation ID** | Track toàn bộ lifecycle của một request |
| **File Rotation** | Tự động rotate theo ngày và kích thước |
| **Auto Cleanup** | Tự động xóa logs cũ theo retention policy |
| **JSON Format** | Dễ parse và tích hợp với log aggregators |
| **Level Filtering** | Filter theo log level |

### Log Levels

| Level | Mô tả | Ví dụ |
|-------|-------|-------|
| `error` | Lỗi nghiêm trọng, cần xử lý ngay | Database connection failed |
| `warn` | Cảnh báo, có thể gây vấn đề | 404 Not Found, deprecated API |
| `info` | Thông tin chung (default) | API Response, Cold Start |
| `debug` | Chi tiết debug | Query execution details |
| `verbose` | Chi tiết tối đa | Full request/response data |

---

## Cấu Trúc Log Files

```
logs/
├── app-YYYY-MM-DD.log          # Tất cả logs
├── error-YYYY-MM-DD.log        # Errors (500+), server vẫn chạy
├── crash-YYYY-MM-DD.log        # Fatal crashes, server died
└── *.gz                        # Compressed old logs
```

### Retention Policy

| Log Type | Retention | Max Size | Compression |
|----------|-----------|----------|-------------|
| `app.log` | 14 days | 20MB/file | ✅ Gzip |
| `error.log` | 30 days | 20MB/file | ✅ Gzip |
| `crash.log` | 30 days | 20MB/file | ✅ Gzip |

### Ví Dụ Thư Mục Logs

```
logs/
├── app-2026-03-16.log          # 249 KB
├── app-2026-03-15.log.gz       # Compressed
├── app-2026-03-14.log.gz       # Compressed
├── error-2026-03-16.log        # Errors (500+)
└── crash-2026-03-16.log        # Fatal crashes (usually empty = healthy)
```

---

## Log Entry Format

### Format Chuẩn

```json
{
  "id": "log_mmtajhqm_003e_0n5p",
  "timestamp": "2026-03-16 21:40:46.211",
  "level": "info",
  "correlationId": "req_1773672046211_8nvhrnd67",
  "context": "Main",
  "message": "API Response",
  "service": "enfyra-server",
  "data": {
    "method": "GET",
    "url": "/logs",
    "statusCode": 200,
    "responseTime": "5ms"
  }
}
```

### Log ID Format

```
log_{timestamp}_{counter}_{random}
│       │          │         │
│       │          │         └── Random 4 chars
│       │          └── Counter (base36, 4 digits)
│       └── Unix timestamp (base36)
└── Prefix
```

**Ví dụ:** `log_mmtajhqm_003e_0n5p`

### Correlation ID Format

```
req_{timestamp}_{random}
│      │          │
│      │          └── Random 9 chars
│      └── Unix timestamp (ms)
└── Prefix
```

**Ví dụ:** `req_1773672046211_8nvhrnd67`

### Khi Nào Có Correlation ID?

| Loại Log | Có Correlation ID? |
|----------|-------------------|
| API Error (400+) | ✅ Có |
| Slow Request (>2s) | ✅ Có |
| Normal Success (200) | ❌ Không logged |
| System Startup | ❌ Không |
| Background Jobs | ❌ Không |

> **Performance Optimization:** Chỉ log khi có lỗi (400+) hoặc request chậm (>2 giây). Success requests bình thường không được log để giảm I/O overhead.

---

## API Endpoints

> **Lưu ý:** Tất cả endpoints yêu cầu authentication (không public).

### 1. Liệt Kê Log Files

```http
GET /logs
```

**Response:**
```json
{
  "files": [
    {
      "name": "app-2026-03-16.log",
      "size": 559506,
      "lineCount": 1116,
      "createdAt": "2026-03-16T13:42:42.584Z",
      "lastModified": "2026-03-16T15:31:16.908Z",
      "compressed": false
    }
  ],
  "stats": {
    "totalSize": 559506,
    "totalSizeFormatted": "546.39 KB",
    "totalLines": 1116,
    "fileCount": 3,
    "oldestFile": "error-2026-03-16.log",
    "newestFile": "app-2026-03-16.log"
  }
}
```

---

### 2. Thống Kê Logs

```http
GET /logs/stats
```

**Response:**
```json
{
  "totalSize": 559506,
  "totalSizeFormatted": "546.39 KB",
  "totalLines": 1116,
  "fileCount": 3,
  "oldestFile": "error-2026-03-16.log",
  "newestFile": "app-2026-03-16.log"
}
```

---

### 3. Đọc Nội Dung Log File

```http
GET /logs/:filename
```

#### Query Parameters

| Parameter | Type | Default | Mô tả |
|-----------|------|---------|-------|
| `page` | number | 1 | Số trang |
| `pageSize` | number | 100 | Số dòng mỗi trang |
| `filter` | string | - | Tìm theo text (case-insensitive) |
| `level` | string | - | Lọc theo level: `error`, `warn`, `info`, `debug` |
| `id` | string | - | Tìm theo Log ID hoặc Correlation ID |
| `correlationId` | string | - | Tìm theo Correlation ID |
| `raw` | boolean | false | Trả về raw JSON strings |

#### Smart ID Detection

Parameter `id` tự động nhận diện:

| ID Prefix | Tìm Theo |
|-----------|----------|
| `log_` | Log Entry ID |
| `req_` | Correlation ID |

#### Response

```json
{
  "file": "app-2026-03-16.log",
  "lines": [
    {
      "id": "log_mmtajhqm_003e_0n5p",
      "timestamp": "2026-03-16 21:40:46.211",
      "level": "info",
      "correlationId": "req_1773672046211_8nvhrnd67",
      "context": "Main",
      "message": "API Response",
      "data": { ... }
    }
  ],
  "totalLines": 150,
  "page": 1,
  "pageSize": 100,
  "hasMore": true
}
```

---

### 4. Tail Log (N Dòng Cuối)

```http
GET /logs/:filename/tail
```

#### Query Parameters

| Parameter | Type | Default | Mô tả |
|-----------|------|---------|-------|
| `lines` | number | 50 | Số dòng cuối |
| `raw` | boolean | false | Trả về raw JSON |

#### Response

```json
{
  "lines": [
    {
      "id": "log_mmtajhqm_003e_0n5p",
      "timestamp": "2026-03-16 21:40:46.211",
      "level": "info",
      "correlationId": "req_1773672046211_8nvhrnd67",
      "message": "API Response"
    }
  ]
}
```

---

## Ví Dụ Sử Dụng

### cURL Examples

#### 1. Xem danh sách log files
```bash
curl -X GET "http://localhost:1105/logs" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

#### 2. Đọc log file (mặc định)
```bash
curl -X GET "http://localhost:1105/logs/app-2026-03-16.log"
```

#### 3. Tìm theo Log ID
```bash
curl -X GET "http://localhost:1105/logs/app-2026-03-16.log?id=log_mmtajhqm_003e_0n5p"
```

#### 4. Tìm theo Correlation ID
```bash
# Cách 1: Dùng correlationId parameter
curl -X GET "http://localhost:1105/logs/app-2026-03-16.log?correlationId=req_1773672046211_8nvhrnd67"

# Cách 2: Dùng id parameter (smart detection)
curl -X GET "http://localhost:1105/logs/app-2026-03-16.log?id=req_1773672046211_8nvhrnd67"
```

#### 5. Lọc theo level
```bash
# Chỉ lấy errors
curl -X GET "http://localhost:1105/logs/app-2026-03-16.log?level=error"

# Chỉ lấy warnings
curl -X GET "http://localhost:1105/logs/app-2026-03-16.log?level=warn"
```

#### 6. Tìm theo text
```bash
curl -X GET "http://localhost:1105/logs/app-2026-03-16.log?filter=database"
```

#### 7. Phân trang
```bash
curl -X GET "http://localhost:1105/logs/app-2026-03-16.log?page=2&pageSize=50"
```

#### 8. Xem N dòng cuối
```bash
curl -X GET "http://localhost:1105/logs/app-2026-03-16.log/tail?lines=20"
```

#### 9. Kết hợp nhiều filter
```bash
curl -X GET "http://localhost:1105/logs/app-2026-03-16.log?level=error&filter=connection&page=1&pageSize=20"
```

---

## Trace Request

### Workflow Trace Một Request

```
┌─────────────────────────────────────────────────────────────┐
│  1. Request đến → Server tạo Correlation ID                  │
│     (hoặc dùng X-Correlation-ID header từ client)           │
├─────────────────────────────────────────────────────────────┤
│  2. Tất cả logs trong request có cùng correlationId          │
│     - Pre-hook logs                                          │
│     - Handler execution logs                                 │
│     - Database queries                                       │
│     - Post-hook logs                                         │
│     - API Response log                                       │
├─────────────────────────────────────────────────────────────┤
│  3. Response trả về với header X-Correlation-ID              │
└─────────────────────────────────────────────────────────────┘
```

### Ví Dụ Trace Thực Tế

**Bước 1:** Gọi API và nhận correlationId từ response header

```bash
curl -i "http://localhost:1105/api/users"
# Response header: X-Correlation-ID: req_1773672046211_8nvhrnd67
```

**Bước 2:** Tìm tất cả logs của request đó

```bash
curl "http://localhost:1105/logs/app-2026-03-16.log?correlationId=req_1773672046211_8nvhrnd67"
```

**Bước 3:** Phân tích kết quả

```json
{
  "lines": [
    {
      "id": "log_abc123_0003",
      "message": "API Response",
      "data": { "statusCode": 200, "responseTime": "45ms" }
    },
    {
      "id": "log_abc123_0002",
      "message": "Database Operation",
      "data": { "table": "users", "duration": "12ms" }
    },
    {
      "id": "log_abc123_0001",
      "message": "Pre-hook: Check permissions",
      "data": { "userId": "42eea198-..." }
    }
  ]
}
```

### Khi Có Error

**Bước 1:** Lấy correlationId từ error response

```json
{
  "success": false,
  "message": "Internal Server Error",
  "error": {
    "correlationId": "req_1773672046211_abc123"
  }
}
```

**Bước 2:** Tìm logs

```bash
# Check error.log trước (chứa 500+ errors)
curl "http://localhost:1105/logs/error-2026-03-16.log?correlationId=req_1773672046211_abc123"

# Nếu không có, check app.log
curl "http://localhost:1105/logs/app-2026-03-16.log?correlationId=req_1773672046211_abc123"
```

**Bước 3:** Xem error details

```json
{
  "lines": [
    {
      "level": "error",
      "message": "Database Query Failed",
      "data": {
        "error": "Connection timeout",
        "query": "SELECT * FROM users"
      }
    }
  ]
}
```

---

## Phân Biệt Error vs Crash

| File | Mục Đích | Khi Nào Ghi |
|------|----------|-------------|
| **error.log** | Application errors | HTTP 500+ responses, handled errors |
| **crash.log** | Fatal crashes | Uncaught exceptions, unhandled rejections, process death |

### Ví Dụ:

**error.log** (Server vẫn chạy):
```javascript
try {
  await database.query();
} catch (error) {
  logger.error('Database query failed', error); // → error.log
  throw new InternalServerErrorException(); // Response 500, server still alive
}
```

**crash.log** (Process died):
```javascript
// Không được catch, process sẽ crash
throw new Error('Uncaught error'); // → crash.log + process exits

// Hoặc unhandled promise rejection
Promise.reject('Unhandled'); // → crash.log + process exits
```

### Sơ Đồ:

```
┌─────────────────────────────────────────────────────────────┐
│                      ERROR OCCURS                           │
├─────────────────────────────────────────────────────────────┤
│                         │                                   │
│    ┌────────────────────┴────────────────────┐             │
│    │                                         │             │
│    ▼                                         ▼             │
│ Handled Error                          Uncaught Error      │
│ (try/catch, caught by filter)          (No handler)        │
│    │                                         │             │
│    ▼                                         ▼             │
│ logger.error()                         Process Crash       │
│    │                                         │             │
│    ▼                                         ▼             │
│ error.log + app.log                    crash.log           │
│ Response 500                           Server Restart      │
└─────────────────────────────────────────────────────────────┘
```

### Status Code Mapping

| Status | Level | Log File | Mô tả |
|--------|-------|----------|-------|
| 400-499 | `warn` | `app.log` | Client errors (404, 401, 403...) |
| 500+ | `error` | `error.log` + `app.log` | Server errors |
| Crash | `error` | `crash.log` | Fatal, process died |

### Health Indicator

```bash
# Server healthy = crash.log trống hoặc không tồn tại
curl "http://localhost:1105/logs/crash-2026-03-16.log/tail?lines=1"
# {"lines": []} ✅ Healthy!

# Có entries = server đã crash, cần investigate
```

---

## Environment Variables

| Variable | Default | Mô tả |
|----------|---------|-------|
| `LOG_DIR` | `./logs` | Thư mục chứa log files |
| `LOG_LEVEL` | `info` | Minimum log level |

### Ví Dụ Cấu Hình

```bash
# .env
LOG_DIR=/var/log/enfyra
LOG_LEVEL=debug
```

---

## Best Practices

### 1. Sử Dụng Correlation ID Từ Client

```javascript
// Frontend
const response = await fetch('/api/users', {
  headers: {
    'X-Correlation-ID': 'my-custom-correlation-id'
  }
});

// Khi có lỗi, dùng correlationId này để trace
```

### 2. Log Levels Phù Hợng

| Level | Khi Nào Dùng |
|-------|--------------|
| `error` | Lỗi cần xử lý ngay, crash, data loss |
| `warn` | 404s, deprecated APIs, retry attempts |
| `info` | Request/response, business events |
| `debug` | Query details, cache hits/misses |
| `verbose` | Full request/response bodies |

### 3. Tìm Kiếm Hiệu Quả

```bash
# Tìm error cụ thể
curl "http://localhost:1105/logs/app-2026-03-16.log?level=error&filter=timeout"

# Trace request có vấn đề
curl "http://localhost:1105/logs/app-2026-03-16.log?correlationId=req_xxx"

# Xem logs mới nhất
curl "http://localhost:1105/logs/app-2026-03-16.log/tail?lines=100"
```

### 4. Monitoring

- Check `error.log` định kỳ
- Set up alerts cho error spikes
- Monitor `totalSize` để detect abnormal logging

---

## Performance Optimization

### Logging Strategy

Hệ thống được tối ưu để giảm performance overhead:

| Condition | Logged? | Reason |
|-----------|---------|--------|
| Status 400+ | ✅ Yes | Errors cần debug |
| Response time > 2s | ✅ Yes | Slow requests cần optimize |
| Normal success (200) | ❌ No | Giảm I/O, improve performance |

### Data Logged

Chỉ log những data cần thiết:

```json
{
  "method": "GET",
  "url": "/api/users",
  "statusCode": 500,
  "responseTime": "150ms",
  "userId": "abc123",
  "query": { "filter": "active" }
}
```

**Không log:**
- Full headers (quá nhiều data)
- Request body cho success requests
- Internal NestJS logs (đã filter)

### Benchmark

| Metric | Trước | Sau |
|--------|-------|-----|
| Logs/request | 1+ | 0 (success) |
| Data/log | ~2KB | ~200B |
| File I/O | Heavy | Minimal |

---

## Troubleshooting

### Không Tìm Thấy Log

1. **Kiểm tra file có tồn tại:**
   ```bash
   curl "http://localhost:1105/logs"
   ```

2. **Kiểm tra correlationId đúng:**
   - Phải bắt đầu bằng `req_`
   - Copy chính xác từ response header hoặc error

3. **Kiểm tra level:**
   ```bash
   curl "http://localhost:1105/logs/app-2026-03-16.log?level=error"
   ```

### Log File Quá Lớn

- Logs tự động rotate khi > 20MB
- Old logs được compress thành `.gz`
- Không thể đọc `.gz` files trực tiếp

### Thiếu Correlation ID

Một số logs không có correlationId:
- System startup logs
- Background job logs
- Logs từ external services

---

## API Summary

| Endpoint | Method | Mô tả |
|----------|--------|-------|
| `/logs` | GET | Liệt kê log files |
| `/logs/stats` | GET | Thống kê logs |
| `/logs/:filename` | GET | Đọc log file |
| `/logs/:filename/tail` | GET | N dòng cuối |

### Quick Reference

```bash
# List files
GET /logs

# Read log
GET /logs/app-2026-03-16.log

# Find by ID
GET /logs/app-2026-03-16.log?id=log_xxx
GET /logs/app-2026-03-16.log?id=req_xxx  # smart detection

# Filter
GET /logs/app-2026-03-16.log?level=error
GET /logs/app-2026-03-16.log?filter=database

# Pagination
GET /logs/app-2026-03-16.log?page=2&pageSize=50

# Tail
GET /logs/app-2026-03-16.log/tail?lines=20
```

### Response Fields

| Field | Description |
|-------|-------------|
| `size` | File size in bytes |
| `lineCount` | Number of log entries |
| `totalLines` | Total lines across all files |
| `totalSizeFormatted` | Human-readable size |