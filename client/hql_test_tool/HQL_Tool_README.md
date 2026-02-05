# HQL执行工具使用说明

## 工具简介
本工具是一个用于执行Hive SQL(HQL)的Python脚本，提供两种主要执行方式：
- 执行HQL文件(--hqlfile)
- 直接执行HQL语句(--hql)

## 环境要求
- Python 3
- 依赖包: pyhive, pandas, chardet, colorama
- Hive服务器访问权限

## 安装方法
```bash
pip install pyhive pandas chardet colorama
```
y6
## 核心参数说明

### 必填参数
`--username`: 执行用户名

### 执行方式 (必须二选一)

#### 1. 执行HQL文件(--hqlfile)
```bash
python test_hql_tool.py --username hduser1004 --hqlfile 文件路径.hql [--date YYYY-MM-DD] [--explain]
```
特点:
- 支持相对路径
- 自动加载同目录.properties文件
- 支持日期参数替换(--date)
- 支持多语句执行

#### 2. 直接执行HQL语句(--hql)
```bash 
python test_hql_tool.py --username hduser1004 --hql "SQL语句"
```
特点:
- 无需文件
- 无需--date参数
- 适合快速测试

### 可选参数
- `--explain`: 生成执行计划(适用于复杂查询)

## 典型使用场景

### 场景1: 执行HQL文件
```bash
# 执行日常报表
python test_hql_tool.py --username hduser1004 --hqlfile daily_report.hql --date 2025-04-30

# 查看执行计划
python test_hql_tool.py --username hduser1004 --hqlfile complex_query.hql --explain
```

### 场景2: 直接执行SQL语句
```bash
# 快速查询数据
python test_hql_tool.py --username hduser1004 --hql "SELECT * FROM table LIMIT 10"

# 创建测试表
python test_hql_tool.py --username hduser1006 --hql "CREATE TABLE IF NOT EXISTS test_table (id INT)"
```

### 场景3: 简短SQL查询(推荐使用--hql)
```bash
# 查询表结构
python test_hql_tool.py --username hduser1004 --hql "DESCRIBE FORMATTED imd_crss_ods_safe.table_name"

# 简单数据查询
python test_hql_tool.py --username hduser1004 --hql "SELECT COUNT(*) FROM imd_dm_safe.user_table"
```

### 场景4: 复杂SQL操作(推荐使用临时文件+--hqlfile)

#### 操作步骤说明：
1. **创建SQL文件**：
   - 使用文本编辑器创建.hql文件
   - 文件内容示例：
     ```sql
     INSERT INTO imd_dm_safe.test_data 
     SELECT * FROM imd_dm_safe.source_table 
     WHERE create_date = '${date}'
     ```
   - 保存为`temp_insert.hql`

2. **执行SQL文件**：
   ```bash
   python test_hql_tool.py --username hduser1006 --hqlfile temp_insert.hql --date 2025-04-30
   ```

3. **清理临时文件(可选)**：
   ```bash
   del temp_insert.hql
   ```

## 支持的用户名
(完整列表见user_configs.json)
