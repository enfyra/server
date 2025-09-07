# Admin Guide

## Overview

This guide is for system administrators managing the Enfyra Backend platform. It includes installation, configuration, monitoring, backup, and troubleshooting.

## System Installation

### System Requirements

#### Minimum

- **CPU**: 2 cores
- **RAM**: 4GB
- **Storage**: 20GB
- **OS**: Ubuntu 20.04+, CentOS 8+, macOS 10.15+

#### Recommended

- **CPU**: 4+ cores
- **RAM**: 8GB+
- **Storage**: 100GB+ SSD
- **OS**: Ubuntu 22.04 LTS

### Install Dependencies

#### Ubuntu/Debian

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Node.js 18+
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt-get install -y nodejs

# Install MySQL
sudo apt install mysql-server -y
sudo systemctl start mysql
sudo systemctl enable mysql

# Install Redis
sudo apt install redis-server -y
sudo systemctl start redis-server
sudo systemctl enable redis-server

# Install PM2
sudo npm install -g pm2

# Install useful tools
sudo apt install -y curl wget git
```

#### CentOS/RHEL

```bash
# Install Node.js
curl -fsSL https://rpm.nodesource.com/setup_18.x | sudo bash -
sudo yum install -y nodejs

# Install MySQL
sudo yum install -y mysql-server
sudo systemctl start mysqld
sudo systemctl enable mysqld

# Install Redis
sudo yum install -y redis
sudo systemctl start redis
sudo systemctl enable redis

# Install PM2
sudo npm install -g pm2
```

#### macOS

```bash
# Install Homebrew (if not already installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Node.js
brew install node@18

# Install MySQL
brew install mysql
brew services start mysql

# Install Redis
brew install redis
brew services start redis

# Install PM2
npm install -g pm2
```

### Install Enfyra Backend

```bash
# Clone repository
git clone <repository-url>
cd enfyra_be

# Install dependencies
npm install

# Create environment file
cp env_example .env

# Configure environment
nano .env
```

## System Configuration

### Database Configuration

#### MySQL

```bash
# Login to MySQL
sudo mysql -u root

# Create database and user
CREATE DATABASE enfyra_cms CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'enfyra'@'localhost' IDENTIFIED BY 'your_secure_password';
GRANT ALL PRIVILEGES ON enfyra_cms.* TO 'enfyra'@'localhost';
FLUSH PRIVILEGES;
EXIT;

# Configure MySQL for production
sudo nano /etc/mysql/mysql.conf.d/mysqld.cnf

# Add/modify the following configurations:
[mysqld]
innodb_buffer_pool_size = 1G
innodb_log_file_size = 256M
innodb_flush_log_at_trx_commit = 2
max_connections = 200
query_cache_size = 64M
query_cache_type = 1

# Restart MySQL
sudo systemctl restart mysql
```

#### PostgreSQL

```bash
# Switch to postgres user
sudo -u postgres psql

# Create database and user
CREATE DATABASE enfyra_cms;
CREATE USER enfyra WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE enfyra_cms TO enfyra;
\q

# Configure PostgreSQL
sudo nano /etc/postgresql/*/main/postgresql.conf

# Add/modify configurations:
shared_buffers = 256MB
effective_cache_size = 1GB
work_mem = 4MB
maintenance_work_mem = 64MB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100

# Restart PostgreSQL
sudo systemctl restart postgresql
```

### Configure Redis

```bash
# Configure Redis
sudo nano /etc/redis/redis.conf

# Add/modify configurations:
maxmemory 512mb
maxmemory-policy allkeys-lru
save 900 1
save 300 10
save 60 10000

# Restart Redis
sudo systemctl restart redis-server
```


## Deployment

### Development Deployment

```bash
# Build application
npm run build

# Run with PM2
pm2 start ecosystem.config.js --env development

# Save PM2 configuration
pm2 save
pm2 startup
```

### Production Deployment

```bash
# Build for production
npm run build:prod

# Run with PM2
pm2 start ecosystem.config.js --env production

# Check status
pm2 status
pm2 logs enfyra-backend
```

### Docker Deployment

```bash
# Build Docker image
docker build -t enfyra-backend .

# Run container
docker run -d \
  --name enfyra-backend \
  -p 1105:1105 \
  --env-file .env \
  --restart unless-stopped \
  enfyra-backend

# Or use Docker Compose
docker-compose up -d
```

## Monitoring and Logging

### PM2 Monitoring

```bash
# View dashboard
pm2 monit

# View logs
pm2 logs enfyra-backend

# View detailed info
pm2 show enfyra-backend

# Restart application
pm2 restart enfyra-backend

# Reload application (zero-downtime)
pm2 reload enfyra-backend
```

### System Monitoring

```bash
# Monitor disk usage
df -h
du -sh /var/log/*

# Monitor memory usage
free -h
cat /proc/meminfo
```

### Database Monitoring

#### MySQL

```sql
-- Check connections
SHOW STATUS LIKE 'Threads_connected';

-- Check slow queries
SHOW VARIABLES LIKE 'slow_query_log';
SHOW VARIABLES LIKE 'long_query_time';

-- Check table sizes
SELECT
  table_name,
  ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'Size (MB)'
FROM information_schema.tables
WHERE table_schema = 'enfyra_cms'
ORDER BY (data_length + index_length) DESC;
```

#### PostgreSQL

```sql
-- Check connections
SELECT count(*) FROM pg_stat_activity;

-- Check slow queries
SELECT query, mean_time, calls
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 10;

-- Check table sizes
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### Log Management

```bash
# Configure log rotation
sudo nano /etc/logrotate.d/enfyra

# Content:
/var/log/enfyra/*.log {
    daily
    missingok
    rotate 52
    compress
    delaycompress
    notifempty
    create 644 www-data www-data
    postrotate
        pm2 reload enfyra-backend
    endscript
}

# Create logs directory
sudo mkdir -p /var/log/enfyra
sudo chown www-data:www-data /var/log/enfyra
```

## Backup and Recovery

### Manual Database Backup

#### MySQL Backup

```bash
# Create backup directory
mkdir -p /backup/mysql

# Manual database backup
DATE=$(date +%Y%m%d_%H%M%S)
mysqldump -u enfyra -p enfyra_cms > /backup/mysql/enfyra_$DATE.sql
gzip /backup/mysql/enfyra_$DATE.sql
```

#### PostgreSQL Backup

```bash
# Create backup directory
mkdir -p /backup/postgres

# Manual database backup
DATE=$(date +%Y%m%d_%H%M%S)
pg_dump -U enfyra enfyra_cms > /backup/postgres/enfyra_$DATE.sql
gzip /backup/postgres/enfyra_$DATE.sql
```

### Recovery

#### Database Recovery

```bash
# MySQL Recovery
mysql -u enfyra -p enfyra_cms < backup/enfyra_20250805_020000.sql

# PostgreSQL Recovery
psql -U enfyra enfyra_cms < backup/enfyra_20250805_020000.sql
```

#### Application Recovery

```bash
# Restore application files
tar -xzf backup/enfyra_app_20250805_030000.tar.gz -C /opt/enfyra_be/

# Restore environment file
cp backup/env_20250805_030000 /opt/enfyra_be/.env

# Restart application
pm2 restart enfyra-backend
```

## Security

### Firewall Configuration

```bash
# Install UFW
sudo apt install ufw

# Configure firewall
sudo ufw default deny incoming
sudo ufw default allow outgoing

# Open required ports
sudo ufw allow ssh
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw allow 1105/tcp  # Only if direct access is needed

# Enable firewall
sudo ufw enable
```

### SSL/TLS Configuration

For SSL/TLS configuration, you will need to configure your reverse proxy (such as Nginx or Apache) based on your specific deployment setup. The application runs on port 1105 by default.

### Database Security

```sql
-- MySQL Security
-- Tạo user với quyền hạn chế
CREATE USER 'enfyra_readonly'@'localhost' IDENTIFIED BY 'password';
GRANT SELECT ON enfyra_cms.* TO 'enfyra_readonly'@'localhost';

-- Xóa user không sử dụng
DROP USER 'test'@'localhost';

-- Kiểm tra users
SELECT user, host FROM mysql.user;
```

## Performance Tuning

### Application Performance

```bash
# Cấu hình Node.js
export NODE_OPTIONS="--max-old-space-size=4096"

# Cấu hình PM2
pm2 start ecosystem.config.js --env production --max-memory-restart 1G
```

### Database Performance

#### MySQL Tuning

```sql
-- Cấu hình InnoDB
SET GLOBAL innodb_buffer_pool_size = 1073741824; -- 1GB
SET GLOBAL innodb_log_file_size = 268435456; -- 256MB
SET GLOBAL innodb_flush_log_at_trx_commit = 2;

-- Create indexes cho các cột thường query
CREATE INDEX idx_products_name ON products(name);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_category ON products(categoryId);
```

#### PostgreSQL Tuning

```sql
-- Cấu hình shared_buffers
ALTER SYSTEM SET shared_buffers = '256MB';

-- Cấu hình effective_cache_size
ALTER SYSTEM SET effective_cache_size = '1GB';

-- Create indexes
CREATE INDEX idx_products_name ON products(name);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_category ON products(category_id);

-- Analyze tables
ANALYZE products;
```

### Redis Performance

```bash
# Configure Redis cho performance
sudo nano /etc/redis/redis.conf

# Thêm/sửa:
maxmemory 1gb
maxmemory-policy allkeys-lru
save 900 1
save 300 10
save 60 10000
tcp-keepalive 300
```

## Troubleshooting

### Common Issues

#### 1. Application not starting

```bash
# Check logs
pm2 logs enfyra-backend

# Check port
sudo netstat -tlnp | grep 1105

# Check process
ps aux | grep node
```

#### 2. Database connection failed

```bash
# Kiểm tra MySQL service
sudo systemctl status mysql

# Kiểm tra PostgreSQL service
sudo systemctl status postgresql

# Test connection
mysql -u enfyra -p enfyra_cms
psql -U enfyra enfyra_cms
```

#### 3. Redis connection failed

```bash
# Kiểm tra Redis service
sudo systemctl status redis-server

# Test connection
redis-cli ping
```

#### 4. High memory usage

```bash
# Kiểm tra memory usage
free -h
ps aux --sort=-%mem | head

# Restart application
pm2 restart enfyra-backend
```

#### 5. High CPU usage

```bash
# Check CPU usage
top

# Kiểm tra slow queries
# MySQL
SHOW PROCESSLIST;

# PostgreSQL
SELECT pid, now() - pg_stat_activity.query_start AS duration, query
FROM pg_stat_activity
WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes';
```

### Log Analysis

```bash
# Xem application logs
pm2 logs enfyra-backend --lines 100

# View system logs
sudo journalctl -u mysql -f
sudo journalctl -u redis-server -f

# View database error logs
sudo tail -f /var/log/mysql/error.log
```

### Health Checks

```bash
# Manual health checks

# Check application
curl -f http://localhost:1105/health

# Check database connection
mysql -u enfyra -p -e "SELECT 1"
# or for PostgreSQL
psql -U enfyra enfyra_cms -c "SELECT 1"

# Check Redis
redis-cli ping
```

## Maintenance

### Regular Maintenance Tasks

```bash
# Manual maintenance tasks

# 1. Database maintenance
mysql -u enfyra -p enfyra_cms -e "OPTIMIZE TABLE products, categories, orders;"
# or for PostgreSQL
psql -U enfyra enfyra_cms -c "VACUUM ANALYZE;"

# 2. Clean old log files
sudo find /var/log -name "*.log.1" -mtime +7 -delete

# 3. Clean old backups (if you create them)
find /backup -name "*.gz" -mtime +30 -delete

# 4. Update system packages
sudo apt update && sudo apt upgrade -y

# 5. Restart application
pm2 reload enfyra-backend
```

## Disaster Recovery

### Backup Strategy

1. **Daily Backups**: Database và application files
2. **Weekly Backups**: Full system backup
3. **Monthly Backups**: Offsite backup
4. **Test Recovery**: Test restore procedures monthly

### Recovery Procedures

```bash
# Manual disaster recovery steps

# 1. Stop application
pm2 stop enfyra-backend

# 2. Restore database from backup
mysql -u enfyra -p enfyra_cms < /backup/enfyra_backup.sql
# or for PostgreSQL
psql -U enfyra enfyra_cms < /backup/enfyra_backup.sql

# 3. Restore application files if needed
# (Application code should come from git repository)
git pull origin main
npm install
npm run build

# 4. Start application
pm2 start enfyra-backend

# 5. Verify recovery
curl -f http://localhost:1105/health
```

## Support and Documentation

### Monitoring Tools

- **PM2**: Application monitoring and process management

### Documentation

- **System Logs**: `/var/log/`
- **Application Logs**: PM2 logs
- **Configuration Files**: `/etc/`
- **Backup Files**: `/backup/`

### Contact Information

- **Emergency**: [Emergency contact]
- **Technical Support**: [Support email]
- **Documentation**: [Documentation URL]
- **Issue Tracker**: [Issue tracker URL]

---

_This guide was last updated: August 2025_
