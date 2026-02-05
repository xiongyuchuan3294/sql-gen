# 本地 Hive 原生安装指南 (macOS M1/Intel - Homebrew篇)

如果您无法使用 Docker，可以通过 Homebrew 在本机直接安装 Hadoop 和 Hive。
**注意**：此过程比 Docker 复杂，涉及 Java 环境配置和多个配置文件修改。

## 1. 安装基础依赖 (Java & Homebrew)

Hive 依赖 Java 8 或 Java 11。推荐此时只安装 Java 8 以避免兼容性问题。

```bash
# 1. 安装 Homebrew (如果尚未安装)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# 2. 安装 OpenJDK 8
brew tap homebrew/cask-versions
brew install --cask homebrew/cask-versions/temurin8
# 或者如果不生效，尝试: brew install openjdk@8

# 3. 配置 Java 环境变量 (添加到 ~/.zshrc)
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)' >> ~/.zshrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.zshrc
source ~/.zshrc

# 验证
java -version
```

## 2. 安装 Hadoop

Hive 需要运行在 Hadoop 之上。

```bash
# 1. 安装 Hadoop
brew install hadoop

# 2. 配置 Hadoop 环境变量 (添加到 ~/.zshrc)
echo 'export HADOOP_HOME=/opt/homebrew/opt/hadoop' >> ~/.zshrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.zshrc
source ~/.zshrc

# 3. 修改 Hadoop 配置 (解决 SSH 本地连接问题)
# 编辑: /opt/homebrew/opt/hadoop/libexec/etc/hadoop/hadoop-env.sh
# 找到 export JAVA_HOME 并修改为具体的路径，例如:
# export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home

# 4. 开启“伪分布式”模式 (修改 core-site.xml)
# 编辑: /opt/homebrew/opt/hadoop/libexec/etc/hadoop/core-site.xml
# 在 <configuration> 中添加:
# <property>
#   <name>fs.defaultFS</name>
#   <value>hdfs://localhost:9000</value>
# </property>
```

## 3. 安装 & 配置 Hive

```bash
# 1. 安装 Hive
brew install hive

# 2. 配置 Hive 环境变量
echo 'export HIVE_HOME=/opt/homebrew/opt/hive' >> ~/.zshrc
echo 'export PATH=$PATH:$HIVE_HOME/bin' >> ~/.zshrc
source ~/.zshrc

# 3. 替换 Guava 包 (关键步骤：解决 Hive 与 Hadoop 版本冲突)
rm $HIVE_HOME/lib/guava-*.jar
cp $HADOOP_HOME/libexec/share/hadoop/common/lib/guava-*.jar $HIVE_HOME/lib/
```

## 4. 初始化元数据库 (使用 Derby 简单模式)

为了测试，我们直接使用内嵌的 Derby 数据库，无需安装 MySQL。

```bash
# 初始化 Schema
schematool -dbType derby -initSchema
```

## 5. 启动与验证

```bash
# 1. 启动 Hadoop (必须)
start-dfs.sh
start-yarn.sh

# 2. 启动 Hive CLI
hive

# 3. 测试 SQL
hive> CREATE TABLE test (id INT, name STRING);
hive> INSERT INTO test VALUES (1, 'Hello');
hive> SELECT * FROM test;
```

## 常见问题
*   **"Connection refused"**: 确保 Hadoop 已启动 (`jps` 命令应显示 NameNode, DataNode)。
*   **"SSH login"**: 首次启动 Hadoop 可能需要开启“远程登录”：系统设置 -> 通用 -> 共享 -> 开启“远程登录”。
